"""Read-only absent-run/deadline detection, isolated from the VM and alert delivery."""

import json
import runpy
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

ROOT=Path(__file__).resolve().parents[2]
M=runpy.run_path(str(ROOT/'functions/monitor/main.py'))


def now(hour,minute=0):
    # 2026-10-30 Berlin is UTC+1.
    return datetime(2026,10,30,hour-1,minute,tzinfo=timezone.utc)


def snapshot(**updates):
    return {'release_id':'accepted','accepted_business_date':'2026-10-30','delivery_status':'current',
            'original_publication_time':now(19,4),'latest_run_business_date':'2026-10-30',
            'latest_run_outcome':'success',**updates}


def codes(*args,**kwargs):
    return {row[1] for row in M['assess'](*args,**kwargs)}


def test_healthy_in_window_run_does_not_alert():
    assert not M['assess'](now(20,15),snapshot(),'RUNNING',True)


def test_entirely_absent_run_and_stopped_vm_are_detected_without_ssh():
    alerts=codes(now(20,15),snapshot(accepted_business_date='2026-10-29',latest_run_business_date='2026-10-29'),
                 'TERMINATED',False)
    assert alerts=={'vm_not_running','run_missing','release_missing'}


def test_manifest_is_valid_run_evidence_when_start_log_was_unavailable():
    assert 'run_missing' not in codes(now(19,20),snapshot(),'RUNNING',False)


def test_deadline_and_vm_start_boundaries():
    missing=snapshot(accepted_business_date='2026-10-29',latest_run_business_date='2026-10-29')
    assert not codes(now(18,44),missing,'TERMINATED',False)
    assert codes(now(18,45),missing,'TERMINATED',False)=={'vm_not_running'}
    assert 'release_missing' not in codes(now(19,59),missing,'RUNNING',True)
    assert 'release_missing' in codes(now(20),missing,'RUNNING',True)
    assert not codes(now(20,30),missing,'TERMINATED',False)


def test_recovered_late_release_retains_deadline_miss():
    assert 'release_late' in codes(now(20,15),snapshot(original_publication_time=now(20,5)),'RUNNING',True)


def test_failed_current_run_and_aging_backlog_have_distinct_severity():
    alerts=M['assess'](now(19,30),snapshot(latest_run_outcome='failed'),'RUNNING',True,301)
    assert ('ERROR','publication_blocked') in {(r[0],r[1]) for r in alerts}
    assert ('WARNING','backlog_aging') in {(r[0],r[1]) for r in alerts}


def test_weekends_and_dst_use_berlin_calendar():
    assert not M['operating_window'](datetime(2026,10,25,19,tzinfo=timezone.utc))
    assert M['operating_window'](datetime(2026,7,3,17,tzinfo=timezone.utc))  # Friday 19:00 summer
    with pytest.raises(ValueError):
        M['operating_window'](datetime(2026,10,30,19))


def test_typed_bigquery_response_and_retry_identity():
    session=Mock()
    session.post.return_value.json.return_value={'jobComplete':True,
        'schema':{'fields':[{'name':'accepted_business_date','type':'DATE'},
                             {'name':'original_publication_time','type':'TIMESTAMP'},
                             {'name':'latest_task_states','type':'JSON'}]},
        'rows':[{'f':[{'v':'2026-10-30'},{'v':str(now(19,4).timestamp())},{'v':'{"producer":"success"}'}]}]}
    result=M['get_snapshot'](session,'unit-project',now(19,15))
    first=session.post.call_args.kwargs['json']
    M['get_snapshot'](session,'unit-project',now(19,16))
    assert first['requestId']==session.post.call_args.kwargs['json']['requestId']
    assert first['maximumBytesBilled']=='67108864'
    assert result['original_publication_time']==now(19,4)
    assert result['latest_task_states']=={'producer':'success'}


def test_unfinished_snapshot_is_not_reported_healthy():
    session=Mock()
    session.post.return_value.json.return_value={'jobComplete':False}
    with pytest.raises(ValueError):
        M['get_snapshot'](session,'unit-project',now(19,15))


def test_alert_is_actionable_and_contains_no_credentials(capsys):
    M['emit']('unit-project',now(20,15),'ERROR','release_missing','Inspect the manifest',
              {'release_id':'accepted','latest_manifest_id':'manifest','latest_run_id':'run'})
    event=json.loads(capsys.readouterr().out)
    assert event['code']=='release_missing' and event['severity']=='ERROR'
    assert event['manifest_id']=='manifest' and 'dashboard_url' in event
    assert len(event['incident_key'])==64


def test_monitor_package_contains_only_its_runtime_inputs(tmp_path):
    pack=runpy.run_path(str(ROOT/'scripts/deploy/package_functions.py'))['package_function']
    runtime,_=pack('monitor',output_dir=tmp_path)
    assert {p.name for p in runtime.iterdir()}=={'main.py','requirements.txt','constraints-python311.txt'}
