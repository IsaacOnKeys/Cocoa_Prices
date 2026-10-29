"""Finite source retries and publish waits within the caller's invocation budget."""

import time
import base64
import io
import hashlib
import json
import os
import re
import uuid
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

INVOCATION_SECONDS = 50
TRANSIENT_STATUS = {408, 429, 500, 502, 503, 504}


def acquisition_identity(source, *, event=None, context=None, key=None):
    """Stable logical key across transport retries; never log caller-supplied keys."""
    try:
        if key is None and event is not None:
            key = event.get("attributes", {}).get("acquisition_key")
        if key is None:
            key = getattr(context, "event_id", None) or uuid.uuid4().hex
        if not isinstance(key, str) or not 1 <= len(key) <= 1024:
            raise ValueError("Invalid key")
        scope = getattr(context, "resource", None)
        encoded = json.dumps([source, scope, key], sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()
    except Exception:
        raise RuntimeError("Invalid acquisition identity") from None


def observation_identity(source, observed, measurements):
    """A business-value revision fingerprint, independent of receipt/acquisition time."""
    encoded = json.dumps([source, observed, measurements], sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def remaining(deadline):
    seconds = deadline - time.monotonic()
    if seconds <= 0:
        raise RuntimeError("Source invocation deadline exceeded")
    return seconds


def retry_delay(response, attempt):
    value = getattr(response, "headers", {}).get("Retry-After", "")
    if value:
        try:
            seconds = int(value)
            if seconds >= 0:
                return seconds
        except ValueError:
            try:
                instant = parsedate_to_datetime(value)
                if instant.tzinfo is not None:
                    return max(0, (instant - datetime.now(timezone.utc)).total_seconds())
            except (ValueError, TypeError, OverflowError):
                pass
    return 0.5 * 2 ** attempt


def get(requests, url, *, deadline, label, **kwargs):
    return request(requests, url, method="get", deadline=deadline, label=label, **kwargs)


def post(requests, url, *, deadline, label, **kwargs):
    """Bound read-only source queries using POST with the same request policy."""
    return request(requests, url, method="post", deadline=deadline, label=label, **kwargs)


def request(requests, url, *, method, deadline, label, **kwargs):
    """Retry transport errors and selected statuses at most three times.

    Never include response bodies, URLs or transport exceptions in errors. Requests
    connect/read limits are inactivity limits; the platform's 60s limit remains
    the final bound for DNS, slow streaming bodies and SDK worker threads.
    """
    for attempt in range(3):
        budget = remaining(deadline)
        response = None
        try:
            response = getattr(requests, method)(
                url, timeout=(min(5, budget / 2), min(20, budget / 2)), **kwargs
            )
        except requests.RequestException:
            failure = f"{label} request failed"
        else:
            remaining(deadline)
            if response.status_code == 200:
                return response
            failure = f"{label} HTTP status {response.status_code}"
            if response.status_code not in TRANSIENT_STATUS:
                raise RuntimeError(failure) from None
        delay = retry_delay(response, attempt)
        if attempt == 2 or delay + 1 >= remaining(deadline):
            raise RuntimeError(failure) from None
        time.sleep(delay)


def publish_timeout(deadline):
    return min(10, remaining(deadline))


class RecoveryRequired(RuntimeError):
    """A bounded automatic acquisition cannot safely bridge the missing interval."""


SOURCE_REQUESTS = {
    'ICCO': {'endpoints': ['https://www.icco.org/statistics/', 'https://www.icco.org/home/',
                          'https://www.icco.org/wp-admin/admin-ajax.php?action=get_wdtable&table_id=26'],
             'series': 'ICCO daily cocoa price', 'units': 'EUR/tonne', 'date_basis': 'source_observation'},
    'EIA': {'endpoints': ['https://api.eia.gov/v2/petroleum/pri/spt/data/', 'https://www.eia.gov/dnav/pet/hist/RBRTED.htm'],
            'frequency': 'daily', 'series': 'RBRTE', 'units': '$/BBL', 'published_table_verified': True},
    'NASA_POWER': {'endpoints': ['https://power.larc.nasa.gov/api/temporal/daily/point'],
                   'parameters': ['PRECTOTCORR', 'GWETROOT'], 'longitude': -6.635, 'latitude': 4.745,
                   'community': 'ag', 'format': 'JSON', 'time_standard': 'LST',
                   'units': {'precipitation': 'mm/day', 'soil_moisture': 'fraction'}},
}
MEASUREMENTS = {'ICCO': ('cocoa_price',), 'EIA': ('oil_price',), 'NASA_POWER': ('precipitation', 'soil_moisture')}


def _json_bytes(document):
    return json.dumps(document, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _archive_json(progress, prefix, document):
    """Create only; a retry may confirm identical existing bytes, never overwrite."""
    from google.api_core.exceptions import PreconditionFailed

    data = _json_bytes(document)
    checksum = hashlib.sha256(data).hexdigest()
    blob = progress.receipts.blob(prefix + checksum + '.json')
    try:
        blob.upload_from_string(data, content_type='application/json', if_generation_match=0,
                                timeout=progress._timeout(), retry=None)
    except PreconditionFailed:
        blob.reload(timeout=progress._timeout(), retry=None)
        if blob.download_as_bytes(if_generation_match=blob.generation, timeout=progress._timeout(), retry=None) != data:
            raise RuntimeError('Daily raw archive conflict') from None
    blob.reload(timeout=progress._timeout(), retry=None)
    return {'bucket': progress.receipts.name, 'object': blob.name, 'generation': str(blob.generation), 'sha256': checksum}


def publish_batch(publisher_factory, project, topic, records, schema, *, source, acquisition_id, acquired_at, deadline, progress):
    """Archive the entire serializable batch before its first transport side effect.

    The prepared archive survives partial publication or process termination.
    A separate immutable outcome records confirmed ACKs; an absent outcome means
    publication unknown, never successful. Metadata is a source-specific allowlist,
    independent of credential-bearing HTTP request objects and SDK exceptions.
    """
    import fastavro

    if len(records) > 90:
        raise RuntimeError('Daily batch exceeds bounded acquisition window')
    messages = []
    for record in records:
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, schema, record)
        data = buffer.getvalue()
        attributes = {'googclient_schemaencoding': 'BINARY', 'acquisition_id': acquisition_id,
                      'observation_id': observation_identity(source, record['date'], {k: record[k] for k in MEASUREMENTS[source]})}
        messages.append({'data': base64.b64encode(data).decode(), 'sha256': hashlib.sha256(data).hexdigest(),
                         'attributes': attributes})
    archived = None
    if progress:
        try:
            schema_bytes = _json_bytes(schema)
            document = {'version': 1, 'stage': 'prepared', 'source': source, 'acquisition_id': acquisition_id,
                        'acquired_at': acquired_at, 'fetched_at': acquired_at,
                        'window_start': progress.start.isoformat(), 'window_end': progress.end.isoformat(),
                        'request': SOURCE_REQUESTS[source], 'schema': schema,
                        'schema_sha256': hashlib.sha256(schema_bytes).hexdigest(), 'encoding': 'avro_binary',
                        'evidence': 'complete_wire_records_with_source_values_and_provenance', 'messages': messages}
            prefix = f'daily-inputs/v1/{source}/{acquired_at[:10]}/{acquisition_id}/'
            if len(_json_bytes(document)) > 16 * 1024 * 1024:
                raise RuntimeError('Daily batch exceeds archive size limit')
            archived = _archive_json(progress, prefix + 'batches/', document)
        except Exception:
            raise RuntimeError('Daily raw input persistence failed; publication withheld') from None
    acknowledged = 0
    failure = 'publisher initialization failed'
    try:
        publisher = publisher_factory()
        destination = publisher.topic_path(project, topic)
        failure = 'Avro publication failed; batch incomplete'
        for message in messages:
            timeout = publish_timeout(deadline)
            publisher.publish(destination, base64.b64decode(message['data']), **message['attributes']).result(
                timeout=min(timeout, publish_timeout(deadline)))
            acknowledged += 1
    except Exception:
        if archived:
            try:
                _archive_json(progress, prefix + 'outcomes/' + archived['sha256'] + '/',
                              {'version': 1, 'stage': 'publication_unconfirmed', 'input': archived,
                               'acknowledged': acknowledged, 'total': len(messages),
                               'recorded_at': datetime.now(timezone.utc).isoformat(), 'error_category': 'publish_failed_or_timed_out'})
            except Exception:
                pass  # Prepared input remains durable; lack of a final outcome is explicitly unknown.
        raise RuntimeError(failure) from None
    if archived:
        try:
            outcome = _archive_json(progress, prefix + 'outcomes/' + archived['sha256'] + '/',
                                    {'version': 1, 'stage': 'publish_acknowledged', 'input': archived,
                                     'acknowledged': acknowledged, 'total': len(messages),
                                     'recorded_at': datetime.now(timezone.utc).isoformat()})
            return {'raw_input': archived, 'raw_publication_outcome': outcome}
        except Exception:
            raise RuntimeError('Daily raw publication outcome persistence failed') from None
    return {}


def coverage(source, today, *, deadline):
    """Enable durable coverage when both bucket settings are supplied.

    Unconfigured local/legacy invocations keep their previous acquisition window.
    Production Terraform supplies both settings; a partial setting fails closed.
    """
    raw, state = os.getenv("COVERAGE_RECEIPT_BUCKET"), os.getenv("COVERAGE_STATE_BUCKET")
    if not raw and not state:
        return None
    if not raw or not state:
        raise RuntimeError("Incomplete acquisition coverage configuration")
    try:
        from google.cloud import storage

        return Coverage(storage.Client(), source, today, raw, state, deadline=deadline)
    except RecoveryRequired:
        raise
    except Exception:
        raise RuntimeError("Acquisition coverage could not be loaded") from None


class Coverage:
    """Publish-acknowledged coverage, separate from sink/release acceptance.

    Immutable receipts retain the actual date/value revision inventory. A compact
    generation-guarded projection selects the next window; it cannot skip holes,
    regress after an out-of-order completion or advance after a partial batch.
    """

    def __init__(self, client, source, today, receipt_bucket, state_bucket, *, deadline):
        if source not in {"ICCO", "EIA", "NASA_POWER"}:
            raise ValueError("Unknown acquisition source")
        self.source, self.today, self.deadline = source, today, deadline
        self.receipts = client.bucket(receipt_bucket)
        self.states = client.bucket(state_bucket)
        self.prefix = f"acquisition-coverage/v1/{source}/"
        self.state, _ = self._read_state()
        if self.state is None and self.receipts.blob(self.prefix + 'initialized.json').exists(timeout=self._timeout(), retry=None):
            raise RecoveryRequired('Coverage projection is missing; rebuild it from retained receipts before acquisition')
        frontier = date.fromisoformat(self.state['window_end']) if self.state else today
        if frontier > today:
            raise RuntimeError("Coverage frontier is in the future")
        self.start = min(today - timedelta(days=30), frontier - timedelta(days=30))
        self.end = today
        if (self.end - self.start).days >= 90:
            raise RecoveryRequired("Acquisition recovery exceeds 90-day bound; controlled backfill required")

    def _timeout(self):
        return min(5, remaining(self.deadline))

    def _read_state(self):
        from google.api_core.exceptions import NotFound

        blob = self.states.blob(self.prefix + "state.json")
        try:
            blob.reload(timeout=self._timeout(), retry=None)
        except NotFound:
            return None, 0
        document = json.loads(blob.download_as_bytes(
            if_generation_match=blob.generation, timeout=self._timeout(), retry=None))
        if document['version'] != 1 or document['source'] != self.source or document['stage'] != 'publish_acknowledged':
            raise RuntimeError("Invalid acquisition coverage state")
        start, end = (date.fromisoformat(document[k]) for k in ('window_start', 'window_end'))
        if start > end or any(date.fromisoformat(document[k]).isoformat() != document[k] for k in ('window_start', 'window_end')):
            raise RuntimeError("Invalid acquisition coverage interval")
        return document, blob.generation

    def complete(self, receipt, observations):
        """Call only after every source row's publication has been acknowledged."""
        from google.api_core.exceptions import PreconditionFailed

        try:
            acquired = datetime.fromisoformat(receipt['acquired_at'])
            if (acquired.tzinfo is None or receipt['source'] != self.source
                    or not re.fullmatch(r'[0-9a-f]{64}', receipt['acquisition_id'])
                    or type(receipt['published']) is not int or receipt['published'] < 0
                    or receipt['status'] != ('published' if observations else 'no_observations')):
                raise ValueError('Invalid coverage receipt')
            inventory = []
            for row in observations:
                observed = date.fromisoformat(row['date'])
                if not self.start <= observed <= self.end or observed.isoformat() != row['date']:
                    raise ValueError("Observation outside coverage window")
                values = {k: v for k, v in row.items() if k != 'date'}
                inventory.append({'date': row['date'], 'observation_id': observation_identity(self.source, row['date'], values),
                                  'missing_measurements': sorted(k for k, v in values.items() if v is None)})
            inventory.sort(key=lambda r: r['date'])
            dates = {r['date'] for r in inventory}
            if len(dates) != len(inventory) or receipt['published'] != len(inventory):
                raise ValueError("Coverage inventory/count mismatch")
            if inventory and (receipt['observation_start'] != inventory[0]['date'] or receipt['observation_end'] != inventory[-1]['date']):
                raise ValueError("Coverage observation range mismatch")
            if not inventory and (receipt['observation_start'] is not None or receipt['observation_end'] is not None):
                raise ValueError('Empty coverage range must be unknown')
            absent = [(self.start + timedelta(days=i)).isoformat() for i in range((self.end-self.start).days+1)
                      if (self.start + timedelta(days=i)).isoformat() not in dates]
            document = {'version': 1, 'stage': 'publish_acknowledged', **receipt, 'source': self.source,
                        'window_start': self.start.isoformat(), 'window_end': self.end.isoformat(),
                        'observations': inventory, 'not_exposed_dates': absent,
                        'absence_basis': 'not_exposed_in_successful_source_response; not inferred market closures'}
            encoded = json.dumps(document, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
            name = self.prefix + 'receipts/' + receipt['acquisition_id'] + '/' + hashlib.sha256(encoded).hexdigest() + '.json'
            blob = self.receipts.blob(name)
            try:
                blob.upload_from_string(encoded, content_type='application/json', if_generation_match=0,
                                        timeout=self._timeout(), retry=None)
            except PreconditionFailed:
                if blob.download_as_bytes(timeout=self._timeout(), retry=None) != encoded:
                    raise RuntimeError("Coverage receipt conflict") from None
            for _ in range(3):
                old, generation = self._read_state()
                lower, upper = self.start, self.end
                if old:
                    before, after = date.fromisoformat(old['window_start']), date.fromisoformat(old['window_end'])
                    if lower > after + timedelta(days=1) or before > upper + timedelta(days=1):
                        raise RuntimeError("Noncontiguous coverage requires reconciliation")
                    lower, upper = min(lower, before), max(upper, after)
                latest = {'acquired_at': receipt['acquired_at'], 'receipt_object': name}
                if old and datetime.fromisoformat(old['acquired_at']) > datetime.fromisoformat(receipt['acquired_at']):
                    latest = {k: old[k] for k in latest}
                state = {'version': 1, 'source': self.source, 'stage': 'publish_acknowledged',
                         'window_start': lower.isoformat(), 'window_end': upper.isoformat(), **latest}
                try:
                    self.states.blob(self.prefix + 'state.json').upload_from_string(
                        json.dumps(state, sort_keys=True), content_type='application/json',
                        if_generation_match=generation, timeout=self._timeout(), retry=None)
                    marker = self.receipts.blob(self.prefix + 'initialized.json')
                    if not marker.exists(timeout=self._timeout(), retry=None):
                        try:
                            marker.upload_from_string(json.dumps({'version': 1, 'source': self.source}),
                                                      content_type='application/json', if_generation_match=0,
                                                      timeout=self._timeout(), retry=None)
                        except PreconditionFailed:
                            pass
                    return {'window_start': self.start.isoformat(), 'window_end': self.end.isoformat(),
                            'coverage_receipt': name, 'coverage_stage': 'publish_acknowledged'}
                except PreconditionFailed:
                    continue
            raise RuntimeError("Concurrent acquisition coverage update exhausted")
        except Exception:
            raise RuntimeError("Acquisition coverage persistence failed") from None
