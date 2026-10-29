"""Isolated native DLQ export/outage/replay probe; never publishes to production inputs."""

import argparse
import hashlib
import json
import re
import runpy
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

PROJECT = 'cocoa-prices-430315'
NUMBER = '494039866722'
LEDGER_BUCKET = 'raw-historic-data'
ROOT = Path(__file__).resolve().parents[2]
RECOVERY = runpy.run_path(str(ROOT / 'scripts/operations/replay_dead_letters.py'))


def names(token):
    if not re.fullmatch(r'20[0-9]{6}_[0-9a-f]{8}', token):
        raise ValueError('Invalid recovery validation token')
    base = 'recovery-validation-' + token.replace('_', '-')
    return {'input': base + '-input', 'source': base + '-input-sub', 'dead': base + '-dead-letter',
            'export': base + '-dead-letter-sub', 'bucket': PROJECT + '-' + base}


def checked(path):
    state = json.loads(path.read_text())
    if state['names'] != names(state['token']):
        raise ValueError('Only isolated validation resources are allowed')
    return state


def clients():
    from google.auth.transport.requests import AuthorizedSession
    from google.cloud import pubsub_v1, storage
    from validate_ingestion import credentials

    credential = credentials()
    return AuthorizedSession(credential), storage.Client(project=PROJECT, credentials=credential), pubsub_v1.PublisherClient(credentials=credential), pubsub_v1.SubscriberClient(credentials=credential)


def api(http, method, path, **kwargs):
    result = http.request(method, 'https://pubsub.googleapis.com/v1/' + path, timeout=30, **kwargs)
    result.raise_for_status()
    return result.json() if result.content else {}


def prepare(path):
    if path.exists():
        raise ValueError('Refusing to replace a recovery receipt')
    token = datetime.now(timezone.utc).strftime('%Y%m%d') + '_' + uuid.uuid4().hex[:8]
    state = {'token': token, 'names': names(token), 'phase': 'preparing'}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2))
    from google.cloud import storage

    http, gcs, publisher, _ = clients()
    n = state['names']
    labels = {'app': 'cocoa-prices', 'purpose': 'recovery-validation'}
    bucket = storage.Bucket(gcs, n['bucket'])
    bucket.iam_configuration.uniform_bucket_level_access_enabled = True
    bucket.iam_configuration.public_access_prevention = 'enforced'
    bucket.versioning_enabled = True
    bucket.soft_delete_policy.retention_duration_seconds = 0
    bucket.labels = labels
    bucket = gcs.create_bucket(bucket, location='europe-west3', timeout=30)
    policy = bucket.get_iam_policy(timeout=10)
    agent = f'serviceAccount:service-{NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com'
    for role in ['roles/storage.objectCreator', 'roles/storage.legacyBucketReader']:
        policy.bindings.append({'role': role, 'members': {agent}})
    bucket.set_iam_policy(policy, timeout=10)
    for name in [n['input'], n['dead']]:
        publisher.create_topic(request={'name': f'projects/{PROJECT}/topics/{name}', 'labels': labels}, timeout=30, retry=None)
    export = api(http, 'PUT', f'projects/{PROJECT}/subscriptions/{n["export"]}', json={
        'topic': f'projects/{PROJECT}/topics/{n["dead"]}', 'labels': labels,
        'ackDeadlineSeconds': 600, 'messageRetentionDuration': '86400s',
        'cloudStorageConfig': {'bucket': n['bucket'], 'filenamePrefix': f'messages/v1/{n["dead"]}/',
                               'filenameSuffix': '.avro', 'maxDuration': '60s', 'maxBytes': '1048576',
                               'avroConfig': {'writeMetadata': True}}})
    state['active_export_before'] = export
    path.write_text(json.dumps(state, indent=2))
    api(http, 'PUT', f'projects/{PROJECT}/subscriptions/{n["source"]}', json={
        'topic': f'projects/{PROJECT}/topics/{n["input"]}', 'labels': labels, 'ackDeadlineSeconds': 10,
        'messageRetentionDuration': '86400s', 'retryPolicy': {'minimumBackoff': '10s', 'maximumBackoff': '60s'},
        'deadLetterPolicy': {'deadLetterTopic': f'projects/{PROJECT}/topics/{n["dead"]}', 'maxDeliveryAttempts': 5}})
    for kind, name, role in [('topics', n['dead'], 'roles/pubsub.publisher'), ('subscriptions', n['source'], 'roles/pubsub.subscriber')]:
        api(http, 'POST', f'projects/{PROJECT}/{kind}/{name}:setIamPolicy', json={'policy': {'bindings': [{'role': role, 'members': [agent]}]}})
    state['phase'] = 'ready'
    path.write_text(json.dumps(state, indent=2))
    print(json.dumps({'phase': 'ready', 'token': token, 'export_state': export['cloudStorageConfig'].get('state')}), flush=True)


def reject_until_export(path, state, http, subscriber, gcs, *, require_denied=False):
    n = state['names']
    deadline = time.monotonic() + 600
    attempts = []
    prefix = f'messages/v1/{n["dead"]}/'
    while time.monotonic() < deadline:
        result = subscriber.pull(request={'subscription': f'projects/{PROJECT}/subscriptions/{n["source"]}', 'max_messages': 1, 'return_immediately': True}, timeout=10, retry=None)
        for received in result.received_messages:
            attempts.append(received.delivery_attempt)
            subscriber.modify_ack_deadline(request={'subscription': f'projects/{PROJECT}/subscriptions/{n["source"]}',
                                                     'ack_ids': [received.ack_id], 'ack_deadline_seconds': 0}, timeout=10, retry=None)
            print(json.dumps({'poison_delivery_attempt': received.delivery_attempt}), flush=True)
        if require_denied and len(attempts) >= 5:
            time.sleep(10)
            meta = api(http, 'GET', f'projects/{PROJECT}/subscriptions/{n["export"]}')
            if meta['cloudStorageConfig'].get('state') == 'PERMISSION_DENIED':
                assert not list(gcs.list_blobs(n['bucket'], prefix=prefix, timeout=10))
                state['denied_export_verified'] = meta
                state['poison_attempts_before_storage_repair'] = attempts
                path.write_text(json.dumps(state, indent=2))
                bucket = gcs.bucket(n['bucket'])
                policy = bucket.get_iam_policy(timeout=10)
                policy.bindings.append({'role': 'roles/storage.objectCreator', 'members': {
                    f'serviceAccount:service-{NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com'}})
                bucket.set_iam_policy(policy, timeout=10)
                require_denied = False
        files = list(gcs.list_blobs(n['bucket'], prefix=prefix, timeout=10))
        known = {f['name'] for f in state.get('exports', [])}
        for blob in files:
            if blob.name in known:
                continue
            data = blob.download_as_bytes(if_generation_match=blob.generation, timeout=30, retry=None)
            checksum = hashlib.sha256(data).hexdigest()
            records = RECOVERY['verified_records'](data, checksum)
            if len(records) != 1:
                raise ValueError('Expected one isolated poison message per export')
            ref = {'name': blob.name, 'generation': str(blob.generation), 'sha256': checksum}
            state.setdefault('exports', []).append(ref)
            state.setdefault('poison_attempts', []).append(attempts)
            path.write_text(json.dumps(state, indent=2))
            return records[0], ref
        time.sleep(5)
    raise RuntimeError('Native recovery probe timed out; retain receipt and reconcile')


def invoke(gcs, publisher, subscriber, ref, bucket_name, *, publish=False, key=None, max_replays=1):
    argv = ['replay_dead_letters.py', 'gs://' + bucket_name + '/' + ref['name'], '--generation', ref['generation'], '--sha256', ref['sha256']]
    if publish:
        argv += ['--publish', '--record', '0', '--topic', subscriber.get_subscription(request={'subscription':
                 f'projects/{PROJECT}/subscriptions/' + ref['name'].split('/')[2].replace('-dead-letter', '-input-sub')}, timeout=10).topic,
                 '--replay-key', key, '--max-replays', str(max_replays), '--ledger-bucket', LEDGER_BUCKET]
    with patch('google.cloud.storage.Client', return_value=gcs), patch('google.cloud.pubsub_v1.PublisherClient', return_value=publisher), patch('google.cloud.pubsub_v1.SubscriberClient', return_value=subscriber), patch.object(sys, 'argv', argv):
        RECOVERY['main']()


def exercise(path):
    state = checked(path)
    if state['phase'] != 'ready':
        raise ValueError('Do not automatically resume an uncertain recovery exercise')
    http, gcs, publisher, subscriber = clients()
    n = state['names']
    state['phase'] = 'exercising'
    path.write_text(json.dumps(state, indent=2))
    bucket = gcs.bucket(n['bucket'])
    policy = bucket.get_iam_policy(timeout=10)
    policy.bindings = [binding for binding in policy.bindings if binding['role'] != 'roles/storage.objectCreator']
    bucket.set_iam_policy(policy, timeout=10)
    # The native state is updated when an export attempts a write. Allow the IAM
    # change to propagate, then drive a real message through the failed export.
    time.sleep(20)
    data = b'\x00original-poison-avro\xff'
    attrs = {'acquisition_id': 'a'*64, 'observation_id': 'b'*64, 'acquisition_key': state['token'],
             'producer_timestamp': '2026-10-29T10:00:00+00:00'}
    state['original_message_id'] = publisher.publish(f'projects/{PROJECT}/topics/{n["input"]}', data, **attrs).result(timeout=30)
    path.write_text(json.dumps(state, indent=2))
    record, ref = reject_until_export(path, state, http, subscriber, gcs, require_denied=True)
    complete_replay(path, state, http, gcs, publisher, subscriber, record, ref)


def complete_replay(path, state, http, gcs, publisher, subscriber, record, ref):
    n = state['names']
    data = b'\x00original-poison-avro\xff'
    attrs = {'acquisition_id': 'a'*64, 'observation_id': 'b'*64, 'acquisition_key': state['token'],
             'producer_timestamp': '2026-10-29T10:00:00+00:00'}
    assert record['data'] == data and all(record['attributes'][k] == v for k, v in attrs.items())
    state['native_data_is_original_bytes'] = True
    state['writer_metadata'] = {k: str(v) for k, v in record.items() if k not in ('data', 'attributes')}
    root, _, _ = RECOVERY['identity'](record, f'projects/{PROJECT}/subscriptions/{n["source"]}', f'projects/{PROJECT}/topics/{n["input"]}')
    state['ledger_root'] = root
    path.write_text(json.dumps(state, indent=2))
    invoke(gcs, publisher, subscriber, ref, n['bucket'])
    invoke(gcs, publisher, subscriber, ref, n['bucket'], publish=True, key=state['token']+'-first')
    again, second_ref = reject_until_export(path, state, http, subscriber, gcs)
    assert again['data'] == data and again['attributes']['recovery_root'] == root
    for pinned in [ref, second_ref]:
        try:
            invoke(gcs, publisher, subscriber, pinned, n['bucket'], publish=True, key=state['token']+'-blocked')
        except ValueError as error:
            assert 'budget' in str(error)
        else:
            raise AssertionError('Poison replay bypassed its default budget')
    invoke(gcs, publisher, subscriber, second_ref, n['bucket'], publish=True, key=state['token']+'-remediated', max_replays=2)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        result = subscriber.pull(request={'subscription': f'projects/{PROJECT}/subscriptions/{n["source"]}', 'max_messages': 1, 'return_immediately': True}, timeout=10, retry=None)
        if result.received_messages:
            received = result.received_messages[0]
            assert received.message.data == data and all(received.message.attributes[k] == v for k, v in attrs.items())
            assert received.message.attributes['recovery_attempt'] == '2'
            subscriber.acknowledge(request={'subscription': f'projects/{PROJECT}/subscriptions/{n["source"]}', 'ack_ids': [received.ack_id]}, timeout=10, retry=None)
            break
        time.sleep(5)
    else:
        raise RuntimeError('Remediated replay not delivered')
    ledger = json.loads(gcs.bucket(LEDGER_BUCKET).blob(f'replay-ledger/v1/{root}.json').download_as_bytes(timeout=10))
    assert len(ledger['attempts']) == 2 and all(a['status'] == 'confirmed' for a in ledger['attempts'])
    state.update(phase='verified', ledger=ledger, original_and_replayed_archives_share_budget=True,
                 exact_bytes_and_business_attributes_preserved=True, storage_denial_recovered_without_loss=True)
    path.write_text(json.dumps(state, indent=2))
    print(json.dumps({'phase': 'verified', 'token': state['token'], 'storage_denial_recovered': True, 'poison_replay_bounded': True}), flush=True)


def resume(path):
    """Continue only a single archived poison before any reserved replay attempt."""
    state = checked(path)
    if state['phase'] != 'exercising' or len(state.get('exports', [])) != 1 or not state.get('denied_export_verified'):
        raise ValueError('Resume requires one verified export after the isolated storage denial')
    http, gcs, publisher, subscriber = clients()
    ref = state['exports'][0]
    if not ref['name'].startswith(f'messages/v1/{state["names"]["dead"]}/'):
        raise ValueError('Unexpected recovery export path')
    data = gcs.bucket(state['names']['bucket']).blob(ref['name'], generation=int(ref['generation'])).download_as_bytes(
        if_generation_match=int(ref['generation']), timeout=30, retry=None)
    records = RECOVERY['verified_records'](data, ref['sha256'])
    if len(records) != 1:
        raise ValueError('Expected exactly one archived poison')
    record = records[0]
    root, _, _ = RECOVERY['identity'](record, f'projects/{PROJECT}/subscriptions/{state["names"]["source"]}',
                                     f'projects/{PROJECT}/topics/{state["names"]["input"]}')
    if gcs.bucket(LEDGER_BUCKET).blob(f'replay-ledger/v1/{root}.json').exists(timeout=10, retry=None):
        raise ValueError('A replay was already reserved; reconcile before any further publication')
    state['resume_verified_no_prior_replay_ledger'] = True
    path.write_text(json.dumps(state, indent=2))
    complete_replay(path, state, http, gcs, publisher, subscriber, record, ref)


def cleanup(path):
    state = checked(path)
    http, gcs, _, _ = clients()
    n = state['names']
    for kind, name in [('subscriptions', n['source']), ('subscriptions', n['export']), ('topics', n['input']), ('topics', n['dead'])]:
        url = f'https://pubsub.googleapis.com/v1/projects/{PROJECT}/{kind}/{name}'
        r = http.get(url, timeout=10)
        if r.status_code == 404:
            continue
        r.raise_for_status()
        if r.json().get('labels', {}).get('purpose') != 'recovery-validation':
            raise ValueError('Refusing to delete an unlabelled Pub/Sub resource')
        r = http.delete(url, timeout=30)
        r.raise_for_status()
    prefix = f'messages/v1/{n["dead"]}/'
    bucket = gcs.get_bucket(n['bucket'], timeout=10)
    if bucket.labels.get('purpose') != 'recovery-validation':
        raise ValueError('Refusing to alter an unlabelled recovery bucket')
    bucket.soft_delete_policy.retention_duration_seconds = 0
    bucket.patch(timeout=10)
    for blob in gcs.list_blobs(n['bucket'], prefix=prefix, versions=True, timeout=10):
        if not blob.name.startswith(prefix):
            raise ValueError('Unexpected probe export prefix')
        blob.delete(if_generation_match=blob.generation, timeout=10)
    root = state.get('ledger_root')
    if root:
        if not re.fullmatch(r'[0-9a-f]{64}', root):
            raise ValueError('Invalid probe ledger root')
        prefix = f'replay-ledger/v1/{root}.json'
        for blob in gcs.list_blobs(LEDGER_BUCKET, prefix=prefix, versions=True, timeout=10):
            if blob.name != prefix:
                raise ValueError('Unexpected probe ledger name')
            ledger = json.loads(blob.download_as_bytes(if_generation_match=blob.generation, timeout=10))
            if ledger['identity']['topic'] != f'projects/{PROJECT}/topics/{n["input"]}':
                raise ValueError('Probe ledger does not belong to the isolated input')
            blob.delete(if_generation_match=blob.generation, timeout=10)
    bucket = gcs.get_bucket(n['bucket'], timeout=10)
    if bucket.labels.get('purpose') != 'recovery-validation':
        raise ValueError('Refusing to delete an unlabelled recovery bucket')
    if list(gcs.list_blobs(bucket, versions=True, timeout=10)):
        raise ValueError('Recovery bucket contains unexpected objects')
    bucket.delete(timeout=30)
    state['resources_deleted'] = True
    path.write_text(json.dumps(state, indent=2))
    print(json.dumps({'resources_deleted': True, 'token': state['token']}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=('prepare', 'exercise', 'resume', 'cleanup'))
    parser.add_argument('--receipt', type=Path, required=True)
    args = parser.parse_args()
    {'prepare': prepare, 'exercise': exercise, 'resume': resume, 'cleanup': cleanup}[args.phase](args.receipt)


if __name__ == '__main__':
    main()
