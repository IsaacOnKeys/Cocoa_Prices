"""Real Functions Framework HTTP/Gen2 event responses with all external IO stubbed."""

import json
import runpy
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import functions_framework
import google.cloud
import pytest

ROOT = Path(__file__).resolve().parents[2]
PACKAGER = runpy.run_path(str(ROOT / 'scripts/deploy/package_functions.py'))
CANARY = 'private-sdk-query-and-body-canary'
EVENT_HEADERS = {
    'ce-specversion': '1.0', 'ce-id': 'unit-event',
    'ce-source': '//pubsub.googleapis.com/projects/unit/topics/trigger',
    'ce-type': 'google.cloud.pubsub.topic.v1.messagePublished',
    'ce-time': '2026-10-28T10:00:00Z',
}


@pytest.fixture
def handler(monkeypatch, tmp_path):
    publisher = Mock()
    publisher.topic_path.return_value = 'projects/unit/topics/output'
    pubsub = ModuleType('google.cloud.pubsub_v1')
    pubsub.PublisherClient = Mock(return_value=publisher)
    secret = ModuleType('google.cloud.secretmanager')
    secret_client = Mock()
    secret_client.access_secret_version.return_value.payload.data = CANARY.encode()
    secret.SecretManagerServiceClient = Mock(return_value=secret_client)
    for module in (pubsub, secret):
        monkeypatch.setitem(sys.modules, module.__name__, module)
        monkeypatch.setattr(google.cloud, module.__name__.rsplit('.', 1)[1], module, raising=False)
    requests = ModuleType('requests')
    requests.RequestException = TimeoutError
    requests.get = Mock()
    monkeypatch.setitem(sys.modules, 'requests', requests)
    monkeypatch.setitem(sys.modules, 'main', ModuleType('placeholder_main'))
    for name in ('FUNCTION_TARGET', 'FUNCTION_SOURCE', 'FUNCTION_SIGNATURE_TYPE', 'EIA_OIL_API_KEY', 'LOG_EXECUTION_ID'):
        monkeypatch.delenv(name, raising=False)

    def create(feed, case='success'):
        runtime, _ = PACKAGER['package_function'](feed, output_dir=tmp_path / 'packages')
        monkeypatch.syspath_prepend(str(runtime))

        def get(url, **kwargs):
            if case == 'http_failure':
                return SimpleNamespace(status_code=403, text=CANARY)
            if case == 'malformed':
                return SimpleNamespace(status_code=200, text=CANARY, json=lambda: {'invalid': CANARY})
            if feed == 'cocoa':
                return SimpleNamespace(status_code=200, text='<table id="table_1"><tbody><tr id="table_1">'
                                       '<td>1</td><td>26/10/2026</td><td>0</td><td>0</td><td>0</td><td>5,075.02</td>'
                                       '</tr></tbody></table>')
            if feed == 'weather':
                return SimpleNamespace(status_code=200, json=lambda: {'properties': {'parameter': {
                    'PRECTOTCORR': {'20261026': 2.5, '20261027': -999},
                    'GWETROOT': {'20261026': 0.4, '20261027': 0.3},
                }}})
            if '/v2/' in url:
                rows = [] if case == 'no_observations' else [
                    {'period': '2026-10-26', 'series': 'RBRTE', 'units': '$/BBL', 'value': '101'},
                    {'period': '2026-10-27', 'series': 'RBRTE', 'units': '$/BBL', 'value': '102'},
                ]
                return SimpleNamespace(status_code=200, json=lambda: {'response': {'data': rows, 'total': str(len(rows))}})
            cells = ['', '', '', '', ''] if case == 'no_observations' else ['101', '102', '', '', '']
            markup = '<title>Europe Brent Spot Price FOB Dollars per Barrel</title><table><tr><td>2026 Oct-26 to Oct-30</td>'
            markup += ''.join('<td>' + value + '</td>' for value in cells) + '</tr></table>'
            return SimpleNamespace(status_code=200, text=markup)

        requests.get.side_effect = get
        target = PACKAGER['FUNCTIONS'][feed][1]
        app = functions_framework.create_app(target=target, source=str(runtime / 'main.py'),
                                             signature_type='http' if feed == 'cocoa' else 'event')

        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 10, 28, 10, tzinfo=timezone.utc)

        sys.modules['main'].datetime = Clock
        if case == 'credentials_failure':
            if feed == 'oil':
                secret_client.access_secret_version.side_effect = RuntimeError(CANARY)
            else:
                pubsub.PublisherClient.side_effect = RuntimeError(CANARY)
        elif case == 'publish_failure':
            publisher.publish.return_value.result.side_effect = RuntimeError(CANARY)
        elif case == 'partial_publish':
            publisher.publish.return_value.result.side_effect = [None, RuntimeError(CANARY)]
        return app.test_client()

    return SimpleNamespace(create=create, publisher=publisher)


def invoke(client, feed):
    return client.post('/', json={} if feed == 'cocoa' else {
        'message': {'data': 'e30=', 'messageId': 'unit-message', 'publishTime': '2026-10-28T10:00:00Z'},
        'subscription': 'projects/unit/subscriptions/trigger',
    }, headers={} if feed == 'cocoa' else EVENT_HEADERS)


@pytest.mark.parametrize('feed', ['cocoa', 'oil', 'weather'])
@pytest.mark.parametrize('case', ['http_failure', 'malformed', 'credentials_failure', 'publish_failure'])
def test_failure_is_http_500_without_complete_receipt_or_secret(handler, capsys, caplog, feed, case):
    response = invoke(handler.create(feed, case), feed)
    assert response.status_code == 500
    output = capsys.readouterr()
    rendered = output.out + output.err + caplog.text + response.get_data(as_text=True)
    assert CANARY not in rendered and 'acquisition_complete' not in rendered


@pytest.mark.parametrize('feed', ['cocoa', 'oil', 'weather'])
def test_success_emits_receipt_only_after_all_publishes(handler, capsys, feed):
    response = invoke(handler.create(feed), feed)
    assert response.status_code == 200
    receipts = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith('{"event": "acquisition_complete"')]
    assert len(receipts) == 1
    assert receipts[0]['status'] == 'published'
    assert receipts[0]['published'] == handler.publisher.publish.call_count == (1 if feed == 'cocoa' else 2)
    assert receipts[0]['observation_start'] == '2026-10-26'
    assert receipts[0]['observation_end'] == ('2026-10-26' if feed == 'cocoa' else '2026-10-27')
    assert receipts[0]['acquired_at'] == '2026-10-28T10:00:00+00:00'
    if feed == 'cocoa':
        assert response.json['date'] == '2026-10-26'  # Valid older source date is preserved.


@pytest.mark.parametrize('feed', ['oil', 'weather'])
def test_partial_publish_is_http_failure_without_success_receipt(handler, capsys, caplog, feed):
    response = invoke(handler.create(feed, 'partial_publish'), feed)
    assert response.status_code == 500 and handler.publisher.publish.call_count == 2
    output = capsys.readouterr()
    assert 'acquisition_complete' not in output.out
    assert CANARY not in output.out + output.err + caplog.text


def test_corroborated_eia_empty_window_is_an_explicit_no_observations_receipt(handler, capsys):
    response = invoke(handler.create('oil', 'no_observations'), 'oil')
    assert response.status_code == 200
    assert '"status": "no_observations", "published": 0' in capsys.readouterr().out
    handler.publisher.publish.assert_not_called()
