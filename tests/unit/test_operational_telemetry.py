"""Structured central logs are bounded, sanitized and independent of delivery success."""

import ast
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT=Path(__file__).resolve().parents[2]


@pytest.fixture
def logger(monkeypatch):
    hook=Mock()
    base=ModuleType('airflow.providers.google.common.hooks.base_google')
    base.GoogleBaseHook=hook
    monkeypatch.setitem(sys.modules,base.__name__,base)
    session=Mock()
    transport=ModuleType('google.auth.transport.requests')
    transport.AuthorizedSession=Mock(return_value=session)
    for name in ('google','google.auth','google.auth.transport'):
        if name not in sys.modules:
            module=ModuleType(name)
            module.__path__=[]
            monkeypatch.setitem(sys.modules,name,module)
    monkeypatch.setitem(sys.modules,transport.__name__,transport)
    scope=dict(GCP_PROJECT='unit-project',hashlib=hashlib,json=json,datetime=datetime,timezone=timezone)
    node=next(n for n in ast.parse((ROOT/'airflow/dags/cocoa_daily.py').read_bytes()).body
              if isinstance(n,ast.FunctionDef) and n.name=='_emit_operational_event')
    exec(compile(ast.Module(body=[node],type_ignores=[]),'operational_log','exec'),scope)
    return SimpleNamespace(write=scope['_emit_operational_event'],session=session,ti=Mock(),hook=hook)


@pytest.mark.parametrize('outcome,severity',[('success','INFO'),('failed','ERROR')])
def test_event_has_searchable_run_identity_and_an_explicit_timeout(logger,outcome,severity):
    payload={'event':'run_completed','dag_run_id':'scheduled-run','release_id':'a'*64,'outcome':outcome,
             'sources':{'cocoa':{'observations':1,'source_ages':{'cocoa_price':1}}}}
    assert logger.write(logger.ti,payload)
    call=logger.session.post.call_args
    assert call.kwargs['timeout']==10
    body=call.kwargs['json']
    assert body['logName']=='projects/unit-project/logs/cocoa-operations'
    assert body['entries'][0]['severity']==severity
    assert body['entries'][0]['jsonPayload']==payload
    assert 'Authorization' not in json.dumps(body)


def test_logging_failure_preserves_delivery_and_hides_sdk_error_text(logger):
    logger.session.post.side_effect=RuntimeError('SECRET_CANARY')
    assert logger.write(logger.ti,{'event':'run_completed','outcome':'success'}) is False
    assert 'SECRET_CANARY' not in str(logger.ti.log.warning.call_args)


def test_credential_failure_is_not_a_new_pipeline_failure(logger):
    logger.hook.side_effect=RuntimeError('SECRET_CANARY')
    assert logger.write(logger.ti,{'event':'run_started'}) is False
    logger.session.post.assert_not_called()


def test_dashboard_reuses_built_in_metrics_and_existing_resource():
    text=(ROOT/'infra/terraform/delivery_monitoring.tf').read_text()
    assert text.count('resource "google_monitoring_dashboard"')==1
    assert 'displayName = "Cocoa analytics operations"' in text
    assert 'logsPanel' in text and 'agent.googleapis.com/memory/percent_used' in text
    assert 'google_logging_metric' not in text and 'custom.googleapis.com' not in text
    assert 'quarantined' in text and 'operational_status' in text
