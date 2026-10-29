"""Run identity, scoped inputs and uncertain BigQuery submissions cannot create a second release."""

import ast
import copy
import hashlib
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from test_required_feed_completion import gate as gate_fixture


@pytest.fixture
def adapter(monkeypatch):
    gate = gate_fixture.__wrapped__(monkeypatch)
    tree = ast.parse((Path(__file__).resolve().parents[2]/'airflow/dags/cocoa_daily.py').read_bytes())
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in {'_release_request','_same_release_job','_run_release_job'}]
    scope = gate.ns
    scope.update(BQ_PROC_FQN='unit-project.analytics_delivery.run_daily_pipeline',
                 RELEASE_PROCEDURE_SHA256=hashlib.sha256(b'verified fixture').hexdigest())
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'atomic_delivery_adapter','exec'),scope)
    accepted = {feed: {'acquisition_id': scope['_expected_acquisition'](feed,gate.run),
                      'acquired_at': '2026-10-29T18:00:03+00:00', 'observations': 31,
                      'receipt_object': 'immutable/'+feed,'receipt_generation':'12','receipt_sha256':'c'*64}
                for feed in ('cocoa','oil','weather')}
    gate.ti.xcom_pull.side_effect = lambda task_ids,key=None: gate.run_scope if task_ids=='capture_baseline' else accepted.get(task_ids.removeprefix('wait_').removesuffix('_rows'))
    request = scope['_release_request'](gate.ti,gate.run,'verified fixture')
    class NotFound(Exception):
        pass
    class Conflict(Exception):
        pass
    exceptions = ModuleType('google.api_core.exceptions')
    exceptions.NotFound,exceptions.Conflict = NotFound,Conflict
    monkeypatch.setitem(sys.modules,exceptions.__name__,exceptions)
    sys.modules['google.cloud.bigquery'].QueryJobConfig = SimpleNamespace(from_api_repr=lambda value: value)
    client = Mock()

    def job(state='RUNNING', error=None):
        value = Mock(state=state,error_result=error)
        value.to_api_repr.return_value = {'configuration':{'query':{'query':request['query'],'queryParameters':copy.deepcopy(request['parameters'])}}}
        value.result.return_value = [{'release_id':request['release_id'],'status':'published',
                                     'model_id':'unit-project.models.versioned','is_active':True}]
        return value
    return SimpleNamespace(scope=scope,gate=gate,accepted=accepted,request=request,client=client,
                           job=job,NotFound=NotFound,transient=gate.transient,permanent=gate.permanent)


def test_request_is_stable_and_contains_exact_accepted_acquisitions(adapter):
    a = adapter
    assert a.scope['_release_request'](a.gate.ti,a.gate.run,'verified fixture') == a.request
    params = {v['name']:v['parameterValue']['value'] for v in a.request['parameters']}
    assert json.loads(params['sources']) == a.accepted and params['stage'] is None
    assert params['run'] == a.gate.run.run_id and params['business'] == '2026-10-29'
    a.scope['RELEASE_PROCEDURE_SHA256'] = '0'*64
    with pytest.raises(a.permanent,match='pin'):
        a.scope['_release_request'](a.gate.ti,a.gate.run,'verified fixture')


def test_missing_current_source_prevents_submission(adapter):
    a = adapter
    a.accepted['weather']['acquisition_id'] = '0'*64
    with pytest.raises(a.permanent,match='current acquisition'):
        a.scope['_release_request'](a.gate.ti,a.gate.run,'verified fixture')
    a.client.query.assert_not_called()


def test_existing_job_is_reattached_with_normalized_null_and_timestamp(adapter):
    a = adapter
    job = a.job()
    params = job.to_api_repr.return_value['configuration']['query']['queryParameters']
    next(p for p in params if p['name']=='stage')['parameterValue'] = {}
    next(p for p in params if p['name']=='started')['parameterValue']['value'] = '2026-10-29 18:00:01+01:00'
    # This is a different instant and must fail before waiting for a result.
    a.client.get_job.return_value = job
    with pytest.raises(a.permanent,match='different request'):
        a.scope['_run_release_job'](a.client,a.request)
    next(p for p in params if p['name']=='started')['parameterValue']['value'] = '2026-10-29 19:00:00+01:00'
    assert a.scope['_run_release_job'](a.client,a.request)['status']=='published'
    a.client.query.assert_not_called()


def test_unknown_lookup_never_chooses_an_alternate_job(adapter):
    a = adapter
    a.client.get_job.side_effect = TimeoutError()
    with pytest.raises(a.transient,match='lookup unavailable'):
        a.scope['_run_release_job'](a.client,a.request)
    a.client.query.assert_not_called()


def test_resubmission_restores_an_explicit_typed_null(adapter):
    a = adapter
    next(p for p in a.request['parameters'] if p['name']=='stage')['parameterValue'] = {}
    a.client.get_job.side_effect = a.NotFound()
    a.client.query.return_value = a.job()
    assert a.scope['_run_release_job'](a.client,a.request)['status']=='published'
    sent = a.client.query.call_args.kwargs['job_config']['query']['queryParameters']
    assert next(p for p in sent if p['name']=='stage')['parameterValue']=={'value':None}


def test_uncertain_submit_reuses_the_original_job_identity(adapter):
    a = adapter
    a.client.get_job.side_effect = a.NotFound()
    a.client.query.side_effect = TimeoutError()
    with pytest.raises(a.transient,match='submission uncertain'):
        a.scope['_run_release_job'](a.client,a.request)
    submitted = a.client.query.call_args.kwargs['job_id']
    a.client.get_job.side_effect = None
    a.client.get_job.return_value = a.job()
    assert a.scope['_run_release_job'](a.client,a.request)['job_id']==submitted
    assert a.client.query.call_count == 1


def test_only_confirmed_transient_failure_permits_second_execution(adapter):
    a = adapter
    a.client.get_job.side_effect = [a.job('DONE',{'reason':'backendError'}),a.NotFound()]
    a.client.query.return_value = a.job()
    result = a.scope['_run_release_job'](a.client,a.request)
    assert result['job_id'].endswith('_1')
    assert a.client.query.call_args.kwargs['job_retry'] is None
    assert a.client.query.call_args.kwargs['job_config']['jobTimeoutMs']=='1200000'


@pytest.mark.parametrize('reason', ['invalidQuery','accessDenied'])
def test_permanent_failure_does_not_execute_another_job(adapter,reason):
    a = adapter
    a.client.get_job.return_value = a.job('DONE',{'reason':reason})
    with pytest.raises(a.permanent,match='failed its contract'):
        a.scope['_run_release_job'](a.client,a.request)
    a.client.query.assert_not_called()


def test_two_confirmed_transient_failures_exhaust_the_budget(adapter):
    a = adapter
    a.client.get_job.return_value = a.job('DONE',{'reason':'internalError'})
    with pytest.raises(a.permanent,match='exhausted'):
        a.scope['_run_release_job'](a.client,a.request)
    assert a.client.get_job.call_count==2
    a.client.query.assert_not_called()
