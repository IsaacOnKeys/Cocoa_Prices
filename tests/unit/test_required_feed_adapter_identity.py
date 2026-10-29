"""The DAG's expected identity matches the real pinned HTTP/Gen2 adapters."""

import json

import pytest

from test_producer_http_failures import EVENT_HEADERS
from test_producer_http_failures import handler as adapter_fixture
from test_required_feed_completion import gate as gate_fixture


@pytest.fixture
def gate(monkeypatch):
    return gate_fixture.__wrapped__(monkeypatch)


@pytest.fixture
def adapter(monkeypatch, tmp_path):
    return adapter_fixture.__wrapped__(monkeypatch, tmp_path)


@pytest.mark.parametrize('feed', ['cocoa', 'oil', 'weather'])
def test_expected_run_identity_matches_real_functions_framework_dispatch(gate, adapter, feed):
    gate.ns['GCP_PROJECT'] = 'unit'
    for item in gate.ns['FEEDS']:
        if item['trigger_topic']:
            item['trigger_topic'] = 'trigger'
    key = f'{gate.run.dag_id}:{gate.run.run_id}:{feed}'
    client = adapter.create(feed)
    if feed == 'cocoa':
        response = client.post('/', headers={'X-Acquisition-Key': key})
    else:
        response = client.post('/', headers=EVENT_HEADERS, data=json.dumps({
            'message': {'data': 'e30=', 'messageId': 'unit-message', 'attributes': {'acquisition_key': key}},
            'subscription': 'projects/unit/subscriptions/trigger'}), content_type='application/json')
    assert response.status_code == 200
    expected = gate.ns['_expected_acquisition'](feed, gate.run)
    assert {call.kwargs['acquisition_id'] for call in adapter.publisher.publish.call_args_list} == {expected}
