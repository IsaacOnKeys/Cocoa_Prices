"""Verify/export an immutable daily batch; publish only on an explicit replay request."""

import argparse
import base64
import hashlib
import io
import json
import re
import runpy
from datetime import date, datetime
from pathlib import Path

import fastavro

ROOT = Path(__file__).resolve().parents[2]
POLICY = runpy.run_path(str(ROOT / 'functions/shared/request_policy.py'))
MAX_BYTES = 16 * 1024 * 1024


def verified_batch(data, checksum):
    """Validate the complete batch before export or the first replay publication."""
    if len(data) > MAX_BYTES or not re.fullmatch(r'[0-9a-f]{64}', checksum) or hashlib.sha256(data).hexdigest() != checksum:
        raise ValueError('Daily input checksum/size mismatch')
    batch = json.loads(data)
    source = batch['source']
    if batch['version'] != 1 or batch['stage'] != 'prepared' or batch['encoding'] != 'avro_binary' or source not in POLICY['MEASUREMENTS']:
        raise ValueError('Unsupported daily input contract')
    acquisition = batch['acquisition_id']
    acquired = datetime.fromisoformat(batch['acquired_at'])
    start, end = (date.fromisoformat(batch[k]) for k in ('window_start', 'window_end'))
    if (not re.fullmatch(r'[0-9a-f]{64}', acquisition) or acquired.tzinfo is None
            or batch['fetched_at'] != batch['acquired_at'] or not 0 <= (end-start).days < 90
            or start.isoformat() != batch['window_start'] or end.isoformat() != batch['window_end']
            or end != acquired.date() or batch['request'] != POLICY['SOURCE_REQUESTS'][source]):
        raise ValueError('Invalid daily input provenance')
    if hashlib.sha256(POLICY['_json_bytes'](batch['schema'])).hexdigest() != batch['schema_sha256']:
        raise ValueError('Daily schema checksum mismatch')
    if not isinstance(batch['messages'], list) or len(batch['messages']) > 90:
        raise ValueError('Invalid daily batch count')
    schema = fastavro.parse_schema(batch['schema'])
    rows, dates = [], set()
    for message in batch['messages']:
        binary = base64.b64decode(message['data'], validate=True)
        if hashlib.sha256(binary).hexdigest() != message['sha256']:
            raise ValueError('Daily message checksum mismatch')
        buffer = io.BytesIO(binary)
        row = fastavro.schemaless_reader(buffer, schema)
        observed = date.fromisoformat(row['date'])
        expected = {'googclient_schemaencoding': 'BINARY', 'acquisition_id': acquisition,
                    'observation_id': POLICY['observation_identity'](source, row['date'], {k: row[k] for k in POLICY['MEASUREMENTS'][source]})}
        if (buffer.read() or not start <= observed <= end or observed.isoformat() != row['date']
                or row['date'] in dates or row['ingestion_time'] != batch['acquired_at'] or message['attributes'] != expected):
            raise ValueError('Daily record provenance/identity mismatch')
        dates.add(row['date'])
        rows.append(row)
    return batch, rows


def replay(batch, publisher, topic, replay_key):
    """Keep acquisition, business revision and source timestamps; label recovery delivery."""
    if not isinstance(replay_key, str) or not 1 <= len(replay_key) <= 1024:
        raise ValueError('An explicit bounded replay key is required')
    if not re.fullmatch(r'projects/[a-z][a-z0-9-]+/topics/[A-Za-z][A-Za-z0-9._~+%-]{2,254}', topic):
        raise ValueError('Expected an explicit fully qualified destination topic')
    replay_id = hashlib.sha256(replay_key.encode()).hexdigest()
    for message in batch['messages']:
        publisher.publish(topic, base64.b64decode(message['data'], validate=True),
                          **message['attributes'], replay_id=replay_id).result(timeout=10)
    return len(batch['messages'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('uri', help='Exact gs://bucket/daily-inputs/v1/.../batches/<SHA256>.json')
    parser.add_argument('--generation', required=True, type=int, help='Generation recorded in the acquisition receipt')
    parser.add_argument('--output', type=Path, help='Create a new local NDJSON file containing the original decoded records')
    parser.add_argument('--publish', action='store_true')
    parser.add_argument('--topic', help='Explicit fully qualified recovery destination')
    parser.add_argument('--replay-key', help='Operator-selected recovery identifier; only its hash is published')
    args = parser.parse_args()
    match = re.fullmatch(r'gs://([^/]+)/(daily-inputs/v1/(ICCO|EIA|NASA_POWER)/[0-9-]+/[0-9a-f]{64}/batches/([0-9a-f]{64})\.json)', args.uri)
    if not match or args.generation <= 0 or (args.publish != bool(args.topic and args.replay_key)) or (not args.publish and (args.topic or args.replay_key)):
        parser.error('Supply an immutable batch URI/generation; publishing also requires --publish, --topic and --replay-key')
    from google.cloud import storage

    blob = storage.Client().bucket(match[1]).blob(match[2], generation=args.generation)
    blob.reload(timeout=10, retry=None)
    if blob.size > MAX_BYTES:
        raise ValueError('Daily input exceeds maximum size')
    batch, rows = verified_batch(blob.download_as_bytes(if_generation_match=args.generation, timeout=30, retry=None), match[4])
    if batch['source'] != match[3]:
        raise ValueError('Archive path/source mismatch')
    if args.output:
        with args.output.open('x', encoding='utf-8', newline='\n') as handle:
            for row in rows:
                handle.write(json.dumps(row, allow_nan=False) + '\n')
    count = 0
    if args.publish:
        from google.cloud import pubsub_v1

        count = replay(batch, pubsub_v1.PublisherClient(), args.topic, args.replay_key)
    print(json.dumps({'source': batch['source'], 'acquisition_id': batch['acquisition_id'], 'acquired_at': batch['acquired_at'],
                      'records_verified': len(rows), 'replayed': count, 'original_provenance_preserved': True}))


if __name__ == '__main__':
    main()
