"""Only the new manifest writer and its terminal-task failure semantics."""

import ast
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pendulum
import pytest

ROOT = Path(__file__).resolve().parents[2]
DAG = ROOT / 'airflow/dags/cocoa_daily.py'


@pytest.fixture
def writer(monkeypatch):
    class Fail(Exception):
        pass

    class NotFound(Exception):
        pass

    class Config:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def to_api_repr(self):
            return {'query': {'queryParameters': self.query_parameters}}

    def parameter(name, kind, value):
        return {'name': name, 'parameterType': {'type': kind}, 'parameterValue': {'value': value}}

    exceptions = ModuleType('google.api_core.exceptions')
    exceptions.NotFound = NotFound
    monkeypatch.setitem(sys.modules, exceptions.__name__, exceptions)
    bq = ModuleType('google.cloud.bigquery')
    bq.QueryJobConfig, bq.ScalarQueryParameter = Config, parameter
    cloud = ModuleType('google.cloud')
    cloud.bigquery = bq
    monkeypatch.setitem(sys.modules, cloud.__name__, cloud)
    monkeypatch.setitem(sys.modules, bq.__name__, bq)
    client = Mock()
    client.get_job.side_effect = NotFound()
    scope = dict(hashlib=hashlib, json=json, re=re, Path=Path, __file__=str(DAG),
                 datetime=datetime, timezone=timezone, TZ=pendulum.timezone('Europe/Berlin'),
                 GCP_PROJECT='unit-project', BQ_LOC='europe-west3',
                 BQ_PROC_FQN='unit-project.analytics_delivery.run_daily_pipeline',
                 BQ_STG={'cocoa': 'unit-project.staging.cocoa'}, COVERAGE_BUCKET='unit-raw',
                 SOURCE_CONTRACT={'cocoa': {'maximum_age': 7}}, PROGRESS_TIMEOUT_MIN=10,
                 PROGRESS_POLL_SEC=20, RELEASE_PROCEDURE_SHA256='a'*64,
                 AirflowFailException=Fail, BigQueryHook=Mock())
    scope['BigQueryHook'].return_value.get_client.return_value = client
    names = {'_utc', '_same_release_job', '_run_manifest_document', 'record_run_manifest_fn'}
    nodes = [node for node in ast.parse(DAG.read_bytes()).body
             if isinstance(node, ast.FunctionDef) and node.name in names]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(DAG), 'exec'), scope)
    run = SimpleNamespace(dag_id='cocoa_daily', run_id='unit-manifest', run_type='scheduled',
                          start_date=datetime(2026, 10, 30, 18, tzinfo=timezone.utc),
                          data_interval_end=datetime(2026, 10, 30, 18, tzinfo=timezone.utc),
                          get_task_instances=Mock(return_value=[SimpleNamespace(task_id='run_bq_pipeline', state='success'),
                                                                SimpleNamespace(task_id='summary', state='running')]))
    ti = Mock(task_id='summary', try_number=1)
    values = {'capture_baseline': {'business_date': '2026-10-30'},
              'wait_cocoa_rows': {'acquisition_id': 'b'*64, 'receipt_generation': '123', 'observations': 4},
              'run_bq_pipeline': {'release_id': 'known'}}
    ti.xcom_pull.side_effect = lambda task_ids, key=None: values.get(task_ids)

    def query(sql, job_config, **kwargs):
        job = Mock()
        job.to_api_repr.return_value = {'configuration': {'query': {'query': sql, **job_config.to_api_repr()['query']}}}
        return job

    client.query.side_effect = query
    return SimpleNamespace(ns=scope, client=client, ti=ti, run=run, values=values, Fail=Fail, NotFound=NotFound)


def test_success_manifest_preserves_exact_input_and_configuration(writer):
    w = writer
    result = w.ns['record_run_manifest_fn'](w.ti, w.run)
    call = w.client.query.call_args
    config = call.kwargs['job_config']
    params = {p['name']: p['parameterValue']['value'] for p in config.query_parameters}
    document = json.loads(params['document'])
    assert document['accepted_sources']['cocoa'] == w.values['wait_cocoa_rows']
    assert document['configuration']['source_contract']['cocoa']['maximum_age'] == 7
    assert document['dag_sha256'] == hashlib.sha256(DAG.read_bytes()).hexdigest()
    assert document['procedure_sha256'] == 'a'*64 and document['wire_contract_version'] == 1
    assert document['outcome'] == 'success' and 'summary' not in document['task_states']
    assert config.maximum_bytes_billed == 20971520
    assert result['manifest_id'].endswith('_1') and call.kwargs['job_retry'] is None
    assert 'JSON_SET' in call.args[0] and 'business_date=@business' in call.args[0]


@pytest.mark.parametrize('state', ['failed', 'upstream_failed', 'skipped', None])
def test_failed_or_missing_upstream_is_archived_and_cannot_turn_dag_green(writer, state):
    w = writer
    w.run.get_task_instances.return_value[0].state = state
    with pytest.raises(w.Fail, match='manifest saved'):
        w.ns['record_run_manifest_fn'](w.ti, w.run)
    params = w.client.query.call_args.kwargs['job_config'].query_parameters
    assert next(p['parameterValue']['value'] for p in params if p['name'] == 'outcome') == 'failed'


def test_prepublication_failure_still_has_run_identity_and_no_release_job_lookup(writer):
    w = writer
    w.values.clear()
    w.run.get_task_instances.return_value[0].state = 'upstream_failed'
    document = w.ns['_run_manifest_document'](w.ti, w.run, w.client)
    assert document['business_date'] == '2026-10-30' and document['run_scope'] is None
    assert len(document['release_id']) == 64 and document['accepted_sources'] == {'cocoa': None}
    assert document['bigquery_jobs'] == []
    w.client.get_job.assert_not_called()


def test_actual_release_jobs_include_billing_metadata_without_error_bodies(writer):
    w = writer
    w.client.get_job.side_effect = [SimpleNamespace(state='DONE', error_result={'reason': 'invalidQuery', 'message': 'sensitive'},
                                                  total_bytes_processed=123, total_bytes_billed=10485760, slot_millis=45),
                                    w.NotFound()]
    document = w.ns['_run_manifest_document'](w.ti, w.run, w.client)
    assert len(document['bigquery_jobs']) == 1
    assert document['bigquery_jobs'][0]['bytes_billed'] == 10485760
    assert document['bigquery_jobs'][0]['error_reason'] == 'invalidQuery'
    assert 'sensitive' not in json.dumps(document)


def test_manifest_storage_failure_does_not_report_success(writer):
    w = writer
    w.client.query.side_effect = TimeoutError('storage unavailable')
    with pytest.raises(TimeoutError):
        w.ns['record_run_manifest_fn'](w.ti, w.run)
    w.ti.log.info.assert_not_called()


def test_configuration_revision_changes_its_fingerprint(writer):
    w = writer
    before = w.ns['_run_manifest_document'](w.ti, w.run, w.client)
    w.ns['PROGRESS_POLL_SEC'] = 30
    after = w.ns['_run_manifest_document'](w.ti, w.run, w.client)
    assert before['configuration_sha256'] != after['configuration_sha256']
    assert before['release_id'] == after['release_id']


def test_existing_manifest_job_is_reattached_without_duplicate_insert(writer):
    w = writer
    w.ns['record_run_manifest_fn'](w.ti, w.run)
    config = w.client.query.call_args.kwargs['job_config']
    sql = w.client.query.call_args.args[0]
    job = Mock()
    job.to_api_repr.return_value = {'configuration': {'query': {'query': sql, **config.to_api_repr()['query']}}}
    w.client.get_job.side_effect = lambda job_id, **_: job if job_id.startswith('cocoa_manifest_') else (_ for _ in ()).throw(w.NotFound())
    w.ns['record_run_manifest_fn'](w.ti, w.run)
    assert w.client.query.call_count == 1
    job.result.assert_called_once()


def test_final_task_waits_for_all_feeds_and_runs_after_failure():
    tree = ast.parse(DAG.read_bytes())
    summary = next(node for node in ast.walk(tree) if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == 'summary' for target in node.targets))
    keywords = {kw.arg: kw.value for kw in summary.value.keywords}
    assert ast.unparse(keywords['trigger_rule']) == 'TriggerRule.ALL_DONE'
    assert ast.unparse(keywords['python_callable']) == 'record_run_manifest_fn'
    assert any(isinstance(node, ast.BinOp) and ast.unparse(node) == 'wait_tasks >> summary' for node in ast.walk(tree))
