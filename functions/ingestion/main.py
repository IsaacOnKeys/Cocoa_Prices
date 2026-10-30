"""Bounded Pub/Sub push -> existing BigQuery staging; Cloud Run IAM authenticates requests."""

import base64
import hashlib
import io
import json
import math
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote

import fastavro

# A maximum-size Pub/Sub message (10 MiB) expands when base64 encoded.
MAX_ENVELOPE_BYTES = 16 * 1024 * 1024
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
    return row


class BusinessInvalid(ValueError):
    def __init__(self, code):
        super().__init__('Business validation failed')
        self.code = code


def validate_business(feed, row):
    try:
        observed = date.fromisoformat(row['date'])
    except (ValueError, TypeError):
        raise BusinessInvalid('invalid_date') from None
    if observed.isoformat() != row['date'] or observed > datetime.now(timezone.utc).date():
        raise BusinessInvalid('invalid_date')
    for key in MEASUREMENTS[feed]:
        value = row[key]
        if value is not None and not math.isfinite(value):
            raise BusinessInvalid('nonfinite')
        if value is not None and ((key == 'cocoa_price' and value <= 0)
                                  or (key == 'precipitation' and value < 0)
                                  or (key == 'soil_moisture' and not 0 <= value <= 1)):
            raise BusinessInvalid('measurement_range')


def validate_observation_identity(feed, row, identity):
    """Verify supplied value revisions; unidentified legacy messages stay explicit."""
    if identity is None:
        return
    source = {'cocoa': 'ICCO', 'oil': 'EIA', 'weather': 'NASA_POWER'}[feed]
    values = {key: row[key] for key in MEASUREMENTS[feed]}
    encoded = json.dumps([source, row['date'], values], sort_keys=True,
                         separators=(',', ':'), allow_nan=False).encode()
    if hashlib.sha256(encoded).hexdigest() != identity:
        raise BusinessInvalid('observation_identity_mismatch')


class SinkRejected(RuntimeError):
    def __init__(self, reasons):
        super().__init__('Staging write rejected')
        self.reasons = tuple(sorted(set(reasons)))
        self.permanent = 'invalid' in self.reasons and set(self.reasons) <= {'invalid', 'stopped'}


def quarantine(envelope, *, feed, delivery_id, identity, category, details, disposition, complete, received, deadline):
    """ACK requires a verified immutable copy; failures preserve transport retry.

    Retain the exact body, never request headers or exception/response bodies.
    Each failed delivery attempt is an immutable event tied to its input hash.
    """
    bucket = os.environ['INGESTION_QUARANTINE_BUCKET']
    if not re.fullmatch(r'[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]', bucket):
        raise ValueError('Invalid quarantine bucket')
    document = {'version': 1, 'source': 'pubsub_push', 'feed': feed, 'delivery_id': delivery_id,
                'acquisition_id': identity.get('acquisition_id'), 'observation_id': identity.get('observation_id'),
                'pubsub_message_id': identity.get('pubsub_message_id'), 'subscription': identity.get('subscription'),
                'received_at': received, 'recorded_at': datetime.now(timezone.utc).isoformat(),
                'error_category': category, 'error_details': details, 'disposition': disposition, 'envelope_complete': complete,
                'input_sha256': hashlib.sha256(envelope).hexdigest(),
                'envelope_base64': base64.b64encode(envelope).decode()}
    data = json.dumps(document, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    checksum = hashlib.sha256(data).hexdigest()
    name = f'quarantine/v1/{received[:10]}/{feed or "unrouted"}/{delivery_id or document["input_sha256"]}/{checksum}.json'
    url = f'https://storage.googleapis.com/storage/v1/b/{bucket}/o/{quote(name, safe="")}'

    def limits():
        seconds = min(10, deadline-time.monotonic())
        if seconds <= 0:
            raise TimeoutError('Quarantine deadline exceeded')
        return {'timeout': (min(3.05, seconds/2), seconds/2), 'max_allowed_time': seconds}

    response = session().post(f'https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o',
                              params={'uploadType': 'media', 'name': name, 'ifGenerationMatch': '0'},
                              headers={'Content-Type': 'application/json'}, data=data, **limits())
    if response.status_code == 412:
        response = session().get(url, **limits())
    response.raise_for_status()
    generation = response.json()['generation']
    if not isinstance(generation, str) or not re.fullmatch(r'[0-9]+', generation):
        raise ValueError('Invalid quarantine generation')
    copy = session().get(url, params={'alt': 'media', 'generation': generation}, **limits())
    copy.raise_for_status()
    if hashlib.sha256(copy.content).hexdigest() != checksum:
        raise RuntimeError('Quarantine checksum mismatch')
    return {'uri': 'gs://'+bucket+'/'+name, 'generation': generation, 'sha256': checksum}


def insert_row(table, row, delivery_id):
    project, dataset, name = table.split(".")
    response = session().post(
        f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{name}/insertAll",
        json={"rows": [{"insertId": delivery_id, "json": row}], "skipInvalidRows": False, "ignoreUnknownValues": False},
        timeout=(3.05, 20), max_allowed_time=30,
    )
    response.raise_for_status()
    result = response.json()
    if not isinstance(result, dict):
        raise RuntimeError("Staging write rejected")
    if result.get('insertErrors'):
        allowed = {'invalid', 'stopped', 'backendError', 'internalError', 'timeout', 'rateLimitExceeded',
                   'quotaExceeded', 'accessDenied', 'notFound', 'resourcesExceeded'}
        reasons = [error.get('reason') if error.get('reason') in allowed else 'other'
                   for row in result['insertErrors'] for error in row.get('errors', [])]
        raise SinkRejected(reasons or ['other'])


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
    feed, delivery_id, identity = None, None, {}
    category, envelope_bytes, complete = 'envelope', b'', False
    received = datetime.now(timezone.utc).isoformat()
    try:
        envelope_bytes = request.stream.read(MAX_ENVELOPE_BYTES + 1)
        complete = len(envelope_bytes) <= MAX_ENVELOPE_BYTES
        if len(envelope_bytes) > MAX_ENVELOPE_BYTES:
            raise ValueError("Oversized envelope")
        envelope = json.loads(envelope_bytes)
        subscription = envelope["subscription"]
        category = 'routing'
        route = routes()[subscription]
        feed = route["feed"]
        category = 'envelope'
        message = envelope["message"]
        message_id = message["messageId"]
        if not isinstance(message_id, str) or not re.fullmatch(r"[0-9]{1,128}", message_id):
            raise ValueError("Invalid message ID")
        if message.get("attributes", {}).get("googclient_schemaencoding") != "BINARY":
            raise ValueError("Expected schema-bound binary message")
        delivery_id = hashlib.sha256(f"{subscription}\0{message_id}".encode()).hexdigest()
        identity = {'subscription': subscription, 'pubsub_message_id': message_id}
        for name in ("acquisition_id", "observation_id"):
            value = message.get("attributes", {}).get(name)
            if value is not None and (not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value)):
                raise ValueError("Invalid identity attribute")
            identity[name] = value
        payload = base64.b64decode(message["data"], validate=True)
        category = 'decode'
        row = decode_row(feed, payload, received)
        category = 'business'
        validate_business(feed, row)
        validate_observation_identity(feed, row, identity['observation_id'])
        row.update(delivery_id=delivery_id, pubsub_message_id=message_id,
                   **{key: identity[key] for key in ('acquisition_id', 'observation_id')})
        category = 'sink'
        insert_row(route["table"], row, delivery_id)
    except Exception as error:
        invalid = category in {'envelope', 'decode', 'business'} and isinstance(
            error, (ValueError, TypeError, KeyError, EOFError, UnicodeError, OverflowError, IndexError, StopIteration))
        permanent = complete and (invalid or isinstance(error, SinkRejected) and error.permanent)
        details = {'error_type': type(error).__name__}
        if isinstance(error, BusinessInvalid):
            details['code'] = error.code
        if isinstance(error, SinkRejected):
            details['reason_codes'] = list(error.reasons)
        status = getattr(getattr(error, 'response', None), 'status_code', None)
        if type(status) is int and 100 <= status <= 599:
            details['http_status'] = status
        try:
            reference = quarantine(envelope_bytes, feed=feed, delivery_id=delivery_id, identity=identity,
                                   category=category, details=details, disposition='quarantined' if permanent else 'retrying',
                                   complete=complete, received=received, deadline=started+50)
        except Exception:
            emit('ingestion_retry', started, feed=feed, delivery_id=delivery_id, error_category=category,
                 evidence_persisted=False, error_type=type(error).__name__)
        else:
            emit('ingestion_quarantined' if permanent else 'ingestion_retry', started, feed=feed,
                 delivery_id=delivery_id, error_category=category, evidence_persisted=True, evidence=reference)
            if permanent:
                return '', 204
        return "Not accepted; retry required", 503
    emit("ingestion_accepted", started, feed=feed, delivery_id=delivery_id, payload_bytes=len(payload),
         timestamp_status=row["timestamp_status"],
         **{key: identity[key] for key in ('acquisition_id', 'observation_id')})
    return "", 204
