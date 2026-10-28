"""Bounded Pub/Sub push -> existing BigQuery staging; Cloud Run IAM authenticates requests."""

import base64
import hashlib
import io
import json
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

import fastavro

MAX_ENVELOPE_BYTES = 2 * 1024 * 1024
MEASUREMENTS = {
    "cocoa": ("cocoa_price",),
    "oil": ("oil_price",),
    "weather": ("precipitation", "soil_moisture"),
}
STARTED = time.monotonic()


@lru_cache(maxsize=1)
def routes():
    project = os.environ["GCP_PROJECT"]
    configured = json.loads(os.environ["INGESTION_ROUTES"])
    if not isinstance(configured, dict) or not configured:
        raise ValueError("No ingestion routes configured")
    for subscription, route in configured.items():
        if not re.fullmatch(rf"projects/{re.escape(project)}/subscriptions/[\w-]+", subscription):
            raise ValueError("Invalid subscription route")
        if route["feed"] not in MEASUREMENTS:
            raise ValueError("Invalid feed route")
        if not re.fullmatch(rf"{re.escape(project)}\.[A-Za-z_]\w*\.[A-Za-z_]\w*", route["table"]):
            raise ValueError("Invalid staging destination")
    return configured


@lru_cache(maxsize=3)
def schema(feed):
    path = Path(__file__).with_name(f"{feed}_schema.avsc")
    return fastavro.parse_schema(json.loads(path.read_bytes()))


@lru_cache(maxsize=1)
def session():
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(credentials, max_refresh_attempts=1)


# All current producers use ISO timestamps with second/microsecond precision.
# Only the verified legacy weather producer used naive datetime.utcnow().
AWARE_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})")
NAIVE_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?")


def timestamp_fields(feed, value, received):
    """Keep acquisition provenance; missing/invalid times never become 'now'."""
    acquired = None
    status = "missing" if value is None else "invalid"
    try:
        if isinstance(value, str) and AWARE_TIMESTAMP.fullmatch(value):
            acquired = datetime.fromisoformat(value.replace("Z", "+00:00"))
            status = "valid"
        elif feed == "weather" and isinstance(value, str) and NAIVE_TIMESTAMP.fullmatch(value):
            acquired = datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
            status = "legacy_naive_utc"
        if acquired is not None:
            acquired = acquired.astimezone(timezone.utc)
            # Bound tolerated source clock skew to five minutes.
            if acquired > datetime.fromisoformat(received.replace("Z", "+00:00")) + timedelta(minutes=5):
                acquired, status = None, "future"
    except (ValueError, OverflowError):
        acquired, status = None, "invalid"
    normalized = acquired.isoformat() if acquired is not None else None
    return {
        "ingestion_time": normalized,
        "producer_acquired_at": normalized,
        "producer_timestamp_raw": value,
        "consumer_received_at": received,
        # These feeds expose observation dates, not verified publication instants.
        "source_published_at": None,
        "timestamp_status": status,
    }


def decode_row(feed, payload, received):
    stream = io.BytesIO(payload)
    record = fastavro.schemaless_reader(stream, schema(feed))
    if stream.read(1):
        raise ValueError("Trailing Avro data")
    row = {
        "date": record["date"],
        **{key: record[key] for key in MEASUREMENTS[feed]},
        **timestamp_fields(feed, record["ingestion_time"], received),
        "raw_payload": record["raw_payload"] or json.dumps(record, ensure_ascii=False, allow_nan=False),
    }
    if any(value is not None and not math.isfinite(value) for key, value in row.items() if key in MEASUREMENTS[feed]):
        raise ValueError("Non-finite measurement")
    return row


def insert_row(table, row, delivery_id):
    project, dataset, name = table.split(".")
    response = session().post(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{name}/insertAll",
        json={"rows": [{"insertId": delivery_id, "json": row}], "skipInvalidRows": False, "ignoreUnknownValues": False},
        timeout=(3.05, 20), max_allowed_time=30,
    )
    response.raise_for_status()
    result = response.json()
    if not isinstance(result, dict) or result.get("insertErrors"):
        raise RuntimeError("Staging write rejected")


def emit(event, started, **fields):
    try:
        import resource

        fields["peak_rss_kib"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except ImportError:
        pass
    print(json.dumps({"event": event, "duration_ms": round((time.monotonic() - started) * 1000),
                      "instance_age_ms": round((time.monotonic() - STARTED) * 1000), **fields}), flush=True)


def stage_analytics_data(request):
    """ACK only a successful staging write; failures remain retryable in Pub/Sub."""
    started = time.monotonic()
    if request.method != "POST":
        return "POST required", 405
    feed, delivery_id = None, None
    try:
        envelope_bytes = request.stream.read(MAX_ENVELOPE_BYTES + 1)
        if len(envelope_bytes) > MAX_ENVELOPE_BYTES:
            raise ValueError("Oversized envelope")
        envelope = json.loads(envelope_bytes)
        subscription = envelope["subscription"]
        route = routes()[subscription]
        feed = route["feed"]
        message = envelope["message"]
        message_id = message["messageId"]
        if not isinstance(message_id, str) or not re.fullmatch(r"[0-9]{1,128}", message_id):
            raise ValueError("Invalid message ID")
        if message.get("attributes", {}).get("googclient_schemaencoding") != "BINARY":
            raise ValueError("Expected schema-bound binary message")
        payload = base64.b64decode(message["data"], validate=True)
        delivery_id = hashlib.sha256(f"{subscription}\0{message_id}".encode()).hexdigest()
        received = datetime.now(timezone.utc).isoformat()
        row = decode_row(feed, payload, received)
        insert_row(route["table"], row, delivery_id)
    except Exception as error:
        # Never log arbitrary exceptions, payloads, tokens, URLs or sink bodies.
        # Invalid data is deliberately not ACKed; durable quarantine/DLQ follows
        # in items 11/12, rather than recreating the old print-and-drop behavior.
        emit("ingestion_retry", started, feed=feed, delivery_id=delivery_id, error_type=type(error).__name__)
        return "Not accepted; retry required", 503
    emit("ingestion_accepted", started, feed=feed, delivery_id=delivery_id, payload_bytes=len(payload),
         timestamp_status=row["timestamp_status"])
    return "", 204
