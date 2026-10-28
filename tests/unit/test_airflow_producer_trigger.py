"""Execute the real trigger callable with external Airflow/Google boundaries stubbed.

The full DAG/provider contract is additionally checked in the native Airflow image.
"""

import ast
import sys
import traceback
import uuid
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def trigger(monkeypatch):
    class AirflowException(Exception):
        pass

    class AirflowFailException(AirflowException):
        pass

    publisher = Mock()
    publisher.get_conn.return_value = publisher
    publisher.topic_path.side_effect = lambda project, topic: f"projects/{project}/topics/{topic}"
    hooks = Mock(return_value=publisher)
    requests = ModuleType('requests')
    requests.head = Mock(return_value=SimpleNamespace(status_code=204))
    requests.post = Mock(return_value=SimpleNamespace(status_code=200, raise_for_status=lambda: None))
    auth_request = ModuleType('google.auth.transport.requests')
    auth_request.Request = Mock()
    oauth = ModuleType('google.oauth2')
    oauth.id_token = SimpleNamespace(fetch_id_token=Mock(return_value='private-token-canary'))
    for module in (requests, auth_request, oauth):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    source = ast.parse((ROOT / 'airflow/dags/cocoa_daily.py').read_bytes())
    function = next(node for node in source.body if isinstance(node, ast.FunctionDef) and node.name == '_invoke_producer')
    namespace = {'PubSubHook': hooks, 'GCP_PROJECT': 'unit-project', 'uuid': uuid,
                 'AirflowException': AirflowException, 'AirflowFailException': AirflowFailException}
    exec(compile(ast.Module(body=[function], type_ignores=[]), 'producer_trigger', 'exec'), namespace)
    return SimpleNamespace(call=namespace['_invoke_producer'], publisher=publisher, requests=requests,
                           transient=AirflowException, permanent=AirflowFailException,
                           run=SimpleNamespace(dag_id='cocoa_daily', run_id='unit-run'))


def test_enqueue_retries_share_a_logical_key_and_bounded_rpc(trigger):
    for _ in range(2):
        assert trigger.call('oil', trigger_topic='oil-trigger', dag_run=trigger.run)['invoked']
    first, second = trigger.publisher.publish.call_args_list
    assert first == second
    assert first.args == ('projects/unit-project/topics/oil-trigger', b'{}')
    assert first.kwargs == {'acquisition_key': 'cocoa_daily:unit-run:oil'}
    publisher_future = trigger.publisher.publish.return_value
    publisher_future.result.assert_called_with(timeout=30)


def test_http_invocation_propagates_the_same_key_and_bounded_timeouts(trigger):
    assert trigger.call('cocoa', url='https://unit.invalid', dag_run=trigger.run)['invoked']
    head, post = trigger.requests.head.call_args, trigger.requests.post.call_args
    assert head.kwargs['headers']['X-Acquisition-Key'] == post.kwargs['headers']['X-Acquisition-Key'] == 'cocoa_daily:unit-run:cocoa'
    assert head.kwargs['timeout'] == (5, 10) and post.kwargs['timeout'] == (5, 65)


@pytest.mark.parametrize('boundary', ['http', 'pubsub'])
def test_transport_failures_are_retryable_failures_without_sensitive_context(trigger, boundary):
    if boundary == 'http':
        trigger.requests.post.side_effect = RuntimeError('private-token-canary')
        arguments = {'url': 'https://unit.invalid'}
    else:
        trigger.publisher.publish.side_effect = RuntimeError('private-token-canary')
        arguments = {'trigger_topic': 'oil-trigger'}
    with pytest.raises(trigger.transient) as error:
        trigger.call('feed', dag_run=trigger.run, **arguments)
    assert not isinstance(error.value, trigger.permanent)
    assert 'private-token-canary' not in ''.join(traceback.format_exception(error.value))


def test_missing_endpoint_is_a_permanent_failure(trigger):
    with pytest.raises(trigger.permanent):
        trigger.call('cocoa', dag_run=trigger.run)


def test_missing_http_endpoint_is_not_silently_skipped(trigger):
    trigger.requests.head.return_value.status_code = 404
    with pytest.raises(trigger.permanent):
        trigger.call('cocoa', url='https://unit.invalid', dag_run=trigger.run)
    trigger.requests.post.assert_not_called()
