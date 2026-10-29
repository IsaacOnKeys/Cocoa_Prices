"""Inspect pinned native Avro exports; explicitly replay one record with a durable budget."""

import argparse
import hashlib
import io
import json
import re
from datetime import datetime, timezone

import fastavro

MAX_BYTES = 32 * 1024 * 1024
MAX_RECORDS = 10000
PREFIX = "CloudPubSubDeadLetterSource"
RECOVERY_KEYS = {"recovery_root", "recovery_attempt", "recovery_original_publish_time", "replay_id"}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def verified_records(data, checksum):
    if len(data) > MAX_BYTES or not re.fullmatch(r"[0-9a-f]{64}", checksum) or digest(data) != checksum:
        raise ValueError("Export checksum/size mismatch")
    records, size = [], 0
    for record in fastavro.reader(io.BytesIO(data)):
        if (set(record) != {"subscription_name", "message_id", "publish_time", "attributes", "data"}
                or not isinstance(record["data"], bytes) or len(record["data"]) > 10 * 1024 * 1024
                or not isinstance(record["publish_time"], datetime) or record["publish_time"].tzinfo is None
                or not isinstance(record["attributes"], dict) or len(record["attributes"]) > 100
                or any(not isinstance(k, str) or not isinstance(v, str) for k, v in record["attributes"].items())
                or not isinstance(record["message_id"], str) or not record["message_id"]):
            raise ValueError("Invalid export record")
        size += len(record["data"]) + len(encoded(record["attributes"]))
        records.append(record)
        if len(records) > MAX_RECORDS or size > MAX_BYTES:
            raise ValueError("Expanded export exceeds recovery limits")
    return records


def identity(record, source, topic):
    attrs = record["attributes"]
    original = attrs.get("recovery_original_publish_time", attrs.get(PREFIX + "TopicPublishTime", ""))
    timestamp = datetime.fromisoformat(original)
    if timestamp.tzinfo is None:
        raise ValueError("Missing original publication timestamp")
    clean = {k: v for k, v in attrs.items() if not k.startswith(PREFIX) and k not in RECOVERY_KEYS}
    # Ordering keys cannot be reconstructed from an attribute with the same name.
    if "ordering_key" in clean:
        raise ValueError("Ordered messages require a dedicated recovery procedure")
    fingerprint = {"source": source, "topic": topic, "data_sha256": digest(record["data"]),
                   "attributes_sha256": digest(encoded(clean)), "original_publish_time": original}
    root = digest(encoded(fingerprint))
    if "recovery_root" in attrs and attrs["recovery_root"] != root:
        raise ValueError("Recovery identity changed")
    return root, fingerprint, clean


def route(record, subscriber, project, dead_letter):
    attrs = record["attributes"]
    source = attrs.get(PREFIX + "Subscription", "")
    if (attrs.get(PREFIX + "SubscriptionProject") != project
            or not re.fullmatch(r"[A-Za-z][A-Za-z0-9._~+%-]{2,254}", source)):
        raise ValueError("Missing or foreign original subscription")
    name = f"projects/{project}/subscriptions/{source}"
    live = subscriber.get_subscription(request={"subscription": name}, timeout=10, retry=None)
    if live.dead_letter_policy.dead_letter_topic != dead_letter:
        raise ValueError("Original subscription no longer forwards to this recovery destination")
    return name, live.topic


def replay(record, *, bucket, publisher, subscriber, project, dead_letter, topic, key, evidence,
           max_replays=1, allow_uncertain=False):
    """Reserve a CAS ledger attempt before sending; uncertain sends consume the budget."""
    from google.api_core.exceptions import NotFound

    if not isinstance(key, str) or not 1 <= len(key) <= 1024 or type(max_replays) is not int or not 1 <= max_replays <= 3:
        raise ValueError("Explicit replay key and a budget from one to three are required")
    source, live_topic = route(record, subscriber, project, dead_letter)
    if topic != live_topic:
        raise ValueError("Explicit destination differs from the original subscription topic")
    root, fingerprint, attrs = identity(record, source, topic)
    ledger = bucket.blob(f"replay-ledger/v1/{root}.json")
    try:
        ledger.reload(timeout=10, retry=None)
        if ledger.size > 16384:
            raise ValueError("Invalid replay ledger size")
        generation = int(ledger.generation)
        state = json.loads(ledger.download_as_bytes(if_generation_match=generation, timeout=10, retry=None))
    except NotFound:
        generation = 0
        state = {"version": 1, "identity": fingerprint, "attempts": []}
    attempts = state["attempts"]
    if state["version"] != 1 or state["identity"] != fingerprint or not isinstance(attempts, list):
        raise ValueError("Replay ledger identity mismatch")
    replay_id = digest(key.encode())
    if (len(attempts) >= max_replays or any(a["replay_id"] == replay_id for a in attempts)
            or (any(a["status"] != "confirmed" for a in attempts) and not allow_uncertain)):
        raise ValueError("Replay budget exhausted, key already used, or uncertain attempt requires reconciliation")
    attrs.update(recovery_root=root, recovery_attempt=str(len(attempts) + 1),
                 recovery_original_publish_time=fingerprint["original_publish_time"], replay_id=replay_id)
    if len(attrs) > 100 or any(len(k.encode()) > 256 or len(v.encode()) > 1024 for k, v in attrs.items()):
        raise ValueError("Recovery attributes exceed Pub/Sub limits")
    attempt = {"replay_id": replay_id, "status": "pending", "reserved_at": datetime.now(timezone.utc).isoformat(),
               "evidence": evidence}
    attempts.append(attempt)
    ledger.upload_from_string(encoded(state), content_type="application/json", if_generation_match=generation, timeout=10, retry=None)
    reserved_generation = int(ledger.generation)
    # An exception here deliberately leaves a pending attempt, never an automatic retry.
    message_id = publisher.publish(topic, record["data"], retry=None, timeout=10, **attrs).result(timeout=30)
    attempt.update(status="confirmed", message_id=message_id, confirmed_at=datetime.now(timezone.utc).isoformat())
    ledger.upload_from_string(encoded(state), content_type="application/json", if_generation_match=reserved_generation, timeout=10, retry=None)
    return {"recovery_root": root, "attempt": len(attempts), "message_id": message_id,
            "ledger_generation": str(ledger.generation)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("uri", help="gs://<recovery-bucket>/messages/v1/<topic>/<export>.avro")
    parser.add_argument("--generation", type=int, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--record", type=int)
    parser.add_argument("--topic")
    parser.add_argument("--replay-key")
    parser.add_argument("--project", default="cocoa-prices-430315")
    parser.add_argument("--ledger-bucket", help="Separate operator recovery bucket; never the export bucket")
    parser.add_argument("--max-replays", type=int, choices=(1, 2, 3), default=1)
    parser.add_argument("--allow-uncertain", action="store_true")
    args = parser.parse_args()
    match = re.fullmatch(r"gs://([a-z0-9][a-z0-9._-]{1,220}[a-z0-9])/(messages/v1/([A-Za-z][A-Za-z0-9._~+%-]{2,254})/[^/]+\.avro)", args.uri)
    if (not match or args.generation <= 0
            or not re.fullmatch(r"[a-z][a-z0-9-]+", args.project)
            or (args.publish and (args.record is None or args.record < 0 or not args.topic or not args.replay_key
                                  or not args.ledger_bucket or args.ledger_bucket == match[1]))
            or (not args.publish and (args.record is not None or args.topic or args.replay_key or args.ledger_bucket or args.max_replays != 1 or args.allow_uncertain))):
        parser.error("Pin export URI/generation/SHA; publishing requires a record, topic, replay key and separate ledger bucket")
    from google.cloud import storage

    project = args.project
    gcs = storage.Client(project=project)
    bucket = gcs.bucket(match[1])
    blob = bucket.blob(match[2], generation=args.generation)
    blob.reload(timeout=10, retry=None)
    if blob.size > MAX_BYTES:
        raise ValueError("Export exceeds maximum size")
    records = verified_records(blob.download_as_bytes(if_generation_match=args.generation, timeout=30, retry=None), args.sha256)
    destination = f"projects/{project}/topics/{match[3]}"
    expected_subscription = match[3] + "-sub"
    valid_subscriptions = {expected_subscription, f"projects/{project}/subscriptions/{expected_subscription}"}
    if any(r["subscription_name"] not in valid_subscriptions for r in records):
        raise ValueError("Export path/subscription mismatch")
    result = {"records_verified": len(records), "data_sha256": [digest(r["data"]) for r in records]}
    if args.publish:
        from google.cloud import pubsub_v1

        if args.record >= len(records):
            raise ValueError("Record index outside the verified export")
        evidence = {"uri": args.uri, "generation": str(args.generation), "sha256": args.sha256, "record": args.record}
        result["replay"] = replay(records[args.record], bucket=gcs.bucket(args.ledger_bucket), publisher=pubsub_v1.PublisherClient(),
                                  subscriber=pubsub_v1.SubscriberClient(), project=project, dead_letter=destination,
                                  topic=args.topic, key=args.replay_key, evidence=evidence,
                                  max_replays=args.max_replays, allow_uncertain=args.allow_uncertain)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
