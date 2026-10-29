"""Every failed delivery is durably quarantined or remains explicitly retryable."""

import base64
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import unquote

import pytest

from test_ingestion import consumer as consumer_fixture, envelope, request


@pytest.fixture
def consumer(monkeypatch, tmp_path):
    return consumer_fixture.__wrapped__(monkeypatch, tmp_path)


class HttpFailure(RuntimeError):
    def __init__(self, status):
        super().__init__('PRIVATE SDK response body')
        self.response = SimpleNamespace(status_code=status)


@pytest.fixture
def storage(consumer, monkeypatch):
    objects, events = {}, []
    config = SimpleNamespace(deny=False, corrupt=False)

    def response(status, body=None, data=b''):
        def check():
            if status >= 400:
                raise HttpFailure(status)
        return SimpleNamespace(status_code=status, json=lambda: body, content=data, raise_for_status=check)

    def post(url, **kwargs):
        assert url == 'https://storage.googleapis.com/upload/storage/v1/b/unit-quarantine/o'
        assert kwargs['params']['ifGenerationMatch'] == '0'
        assert 0 < kwargs['max_allowed_time'] <= 10 and all(t > 0 for t in kwargs['timeout'])
        if config.deny:
            return response(403)
        name = kwargs['params']['name']
        assert name.startswith('quarantine/v1/')
        if name in objects:
            return response(412)
        objects[name] = kwargs['data']
        events.append('persisted')
        return response(200, {'generation': '123'})

    def get(url, **kwargs):
        name = unquote(url.split('/o/', 1)[1])
        if kwargs.get('params', {}).get('alt') == 'media':
            assert kwargs['params']['generation'] == '123'
            events.append('verified')
            return response(200, data=b'corrupt' if config.corrupt else objects[name])
        return response(200, {'generation': '123'})

    monkeypatch.setenv('INGESTION_QUARANTINE_BUCKET', 'unit-quarantine')
    monkeypatch.setattr(consumer, 'session', lambda: SimpleNamespace(post=post, get=get))
    sink = Mock()
    monkeypatch.setattr(consumer, 'insert_row', sink)
    return SimpleNamespace(objects=objects, events=events, sink=sink, config=config)


def stored(storage):
    assert len(storage.objects) == 1
    name, data = next(iter(storage.objects.items()))
    assert name.endswith('/'+hashlib.sha256(data).hexdigest()+'.json')
    document = json.loads(data)
    original = base64.b64decode(document['envelope_base64'], validate=True)
    assert hashlib.sha256(original).hexdigest() == document['input_sha256']
    return document, original


@pytest.mark.parametrize('feed', ['cocoa', 'oil', 'weather'])
def test_malformed_avro_is_acknowledged_only_after_exact_evidence_is_verified(consumer, storage, capsys, feed):
    body = envelope(feed, payload=b'\xff')
    body['message']['attributes'].update(acquisition_id='a'*64, observation_id='b'*64)
    req = request(body)
    original = req.stream.getvalue()
    assert consumer.stage_analytics_data(req)[1] == 204
    assert storage.events == ['persisted', 'verified']
    storage.sink.assert_not_called()
    document, recovered = stored(storage)
    assert recovered == original and document['envelope_complete']
    assert document['error_category'] == 'decode' and document['disposition'] == 'quarantined'
    assert document['pubsub_message_id'] == body['message']['messageId']
    assert document['acquisition_id'] == 'a'*64 and document['observation_id'] == 'b'*64
    assert 'ingestion_quarantined' in capsys.readouterr().out


@pytest.mark.parametrize('feed,changes,code', [
    ('oil', {'date': None}, 'invalid_date'), ('oil', {'date': '20261020'}, 'invalid_date'),
    ('oil', {'date': '2099-01-01'}, 'invalid_date'), ('oil', {'oil_price': float('nan')}, 'nonfinite'),
    ('cocoa', {'cocoa_price': 0}, 'measurement_range'), ('cocoa', {'cocoa_price': -1}, 'measurement_range'),
    ('weather', {'precipitation': -1}, 'measurement_range'), ('weather', {'soil_moisture': 1.01}, 'measurement_range'),
])
def test_valid_avro_with_bad_business_data_is_durably_quarantined(consumer, storage, feed, changes, code):
    values = {'cocoa': {'cocoa_price': 6500.25}, 'oil': {'oil_price': 72.5}, 'weather': {'precipitation': 3.5, 'soil_moisture': 0.42}}
    record = {'date': '2026-10-20', **values[feed], 'ingestion_time': '2026-10-20T10:15:30Z', 'raw_payload': 'PRIVATE source body', **changes}
    assert consumer.stage_analytics_data(request(envelope(feed, record)))[1] == 204
    storage.sink.assert_not_called()
    evidence, _ = stored(storage)
    assert evidence['error_category'] == 'business' and evidence['error_details']['code'] == code


def test_permanent_sink_rejection_is_retained_instead_of_retried_forever(consumer, storage, capsys):
    storage.sink.side_effect = consumer.SinkRejected(['invalid', 'stopped'])
    assert consumer.stage_analytics_data(request(envelope()))[1] == 204
    evidence, _ = stored(storage)
    assert evidence['error_category'] == 'sink' and evidence['disposition'] == 'quarantined'
    assert evidence['error_details']['reason_codes'] == ['invalid', 'stopped']
    logs = capsys.readouterr().out
    assert 'ingestion_accepted' not in logs


@pytest.mark.parametrize('failure', ['timeout', 'http403', 'transient_row', 'unknown'])
def test_transient_or_ambiguous_sink_failure_retains_input_and_returns_retry(consumer, storage, capsys, failure):
    errors = {'timeout': TimeoutError('PRIVATE timeout'), 'http403': HttpFailure(403),
              'transient_row': consumer.SinkRejected(['backendError']), 'unknown': RuntimeError('PRIVATE sink error')}
    storage.sink.side_effect = errors[failure]
    assert consumer.stage_analytics_data(request(envelope()))[1] == 503
    evidence, _ = stored(storage)
    assert evidence['error_category'] == 'sink' and evidence['disposition'] == 'retrying'
    if failure == 'http403':
        assert evidence['error_details']['http_status'] == 403
    logs = capsys.readouterr().out
    assert 'PRIVATE' not in logs and 'ingestion_accepted' not in logs


@pytest.mark.parametrize('failure', ['denied', 'corrupt', 'missing_config'])
def test_unverified_quarantine_never_acknowledges_a_bad_input(consumer, storage, monkeypatch, capsys, failure):
    storage.config.deny = failure == 'denied'
    storage.config.corrupt = failure == 'corrupt'
    if failure == 'missing_config':
        monkeypatch.delenv('INGESTION_QUARANTINE_BUCKET')
    assert consumer.stage_analytics_data(request(envelope(payload=b'\xff')))[1] == 503
    storage.sink.assert_not_called()
    logs = capsys.readouterr().out
    assert 'ingestion_quarantined' not in logs and 'PRIVATE' not in logs


def test_oversized_truncated_evidence_is_explicitly_retrying(consumer, storage, monkeypatch):
    monkeypatch.setattr(consumer, 'MAX_ENVELOPE_BYTES', 10)
    assert consumer.stage_analytics_data(request(envelope()))[1] == 503
    evidence, original = stored(storage)
    assert not evidence['envelope_complete'] and len(original) == 11
    assert evidence['disposition'] == 'retrying'


def test_unknown_route_is_recoverable_configuration_failure(consumer, storage):
    body = envelope()
    body['subscription'] = 'projects/unit-project/subscriptions/unconfigured'
    assert consumer.stage_analytics_data(request(body))[1] == 503
    assert stored(storage)[0]['error_category'] == 'routing'
    assert stored(storage)[0]['disposition'] == 'retrying'


def test_healthy_nullable_input_does_not_create_quarantine_evidence(consumer, storage):
    record = {'date': '2026-10-20', 'oil_price': None, 'ingestion_time': '2026-10-20T10:15:30Z', 'raw_payload': None}
    assert consumer.stage_analytics_data(request(envelope(record=record)))[1] == 204
    storage.sink.assert_called_once()
    assert not storage.objects
