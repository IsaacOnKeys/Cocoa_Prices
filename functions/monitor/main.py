"""Read-only watchdog outside the Airflow VM. All emitted alerts are structured logs."""

import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import functions_framework

BERLIN = ZoneInfo('Europe/Berlin')


def operating_window(now):
    if now.tzinfo is None:
        raise ValueError('Monitor timestamps must be timezone aware')
    local = now.astimezone(BERLIN)
    return local.weekday() < 5 and (18,45) <= (local.hour,local.minute) < (20,30)


def assess(now, snapshot, vm_status, started, oldest_message_seconds=0):
    local = now.astimezone(BERLIN)
    day = local.date().isoformat()
    alerts = []
    if not operating_window(now):
        return alerts
    if vm_status != 'RUNNING':
        alerts.append(('ERROR','vm_not_running','Airflow VM is not running during its operating window.'))
    run_recorded = snapshot.get('latest_run_business_date') == day and snapshot.get('latest_run_outcome') in {'success','failed'}
    if (local.hour,local.minute) >= (19,15) and not (started or run_recorded):
        alerts.append(('ERROR','run_missing','No source-run start was recorded for this business date.'))
    if snapshot.get('latest_run_business_date') == day and snapshot.get('latest_run_outcome') == 'failed':
        alerts.append(('ERROR','publication_blocked','The latest run failed; inspect its manifest and task states.'))
    deadline = datetime(local.year,local.month,local.day,20,0,tzinfo=BERLIN)
    if local >= deadline:
        if (snapshot.get('accepted_business_date') != day or snapshot.get('delivery_status') != 'current'
                or snapshot.get('original_publication_time') is None):
            alerts.append(('ERROR','release_missing','No complete accepted release by the 20:00 Berlin deadline.'))
        elif snapshot.get('original_publication_time') and snapshot['original_publication_time'] > deadline:
            alerts.append(('ERROR','release_late','A release arrived after its deadline; recovery does not erase the miss.'))
    if oldest_message_seconds > 300:
        alerts.append(('WARNING','backlog_aging','A source/consumer queue has messages older than five minutes.'))
    return alerts


def emit(project, now, severity, code, message, snapshot=None):
    record = {'severity':severity,'event':'operational_alert','code':code,'message':message,
              'business_date':now.astimezone(BERLIN).date().isoformat(),
              'incident_key':hashlib.sha256((now.astimezone(BERLIN).date().isoformat()+':'+code).encode()).hexdigest(),
              'dashboard_url':'https://console.cloud.google.com/monitoring/dashboards/builder/b5f39305-22ae-44ea-983e-ffb23c40de73?project='+project}
    if snapshot:
        record.update(release_id=snapshot.get('release_id'),manifest_id=snapshot.get('latest_manifest_id'),
                      dag_run_id=snapshot.get('latest_run_id'),task_states=snapshot.get('latest_task_states'))
    print(json.dumps(record,default=str))


def get_snapshot(session, project, now):
    sql = f'''SELECT release_id,accepted_business_date,original_publication_time,delivery_status,
      latest_manifest_id,latest_run_id,latest_run_outcome,latest_run_business_date,latest_task_states
      FROM `{project}.cocoa_related_studio.operational_status`'''
    request_id = str(uuid.uuid5(uuid.NAMESPACE_URL,project+':monitor:'+str(int(now.timestamp())//900)))
    response = session.post(f'https://bigquery.googleapis.com/bigquery/v2/projects/{project}/queries',json={
        'query':sql,'useLegacySql':False,'location':'europe-west3','maximumBytesBilled':'67108864',
        'timeoutMs':10000,'requestId':request_id,'labels':{'app':'cocoa-prices','component':'watchdog'}},timeout=15)
    response.raise_for_status()
    data = response.json()
    if data.get('errors') or not data.get('jobComplete') or len(data.get('rows',[])) != 1:
        raise ValueError('Operational snapshot did not complete')
    result = {}
    for field, cell in zip(data['schema']['fields'],data['rows'][0]['f']):
        value = cell.get('v')
        if value is not None and field['type'] == 'TIMESTAMP':
            value = datetime.fromtimestamp(float(value),timezone.utc)
        if value is not None and field['type'] == 'JSON':
            value = json.loads(value)
        result[field['name']] = value
    return result


@functions_framework.http
def check_analytics_delivery(request):
    now = datetime.now(timezone.utc)
    probe = (request.get_json(silent=True) or {}).get('probe') is True
    if not operating_window(now) and not probe:
        return {'status':'outside_operating_window'},200
    project = os.environ['GCP_PROJECT']
    if not re.fullmatch(r'[a-z][a-z0-9-]{4,29}',project):
        raise ValueError('Invalid monitor project')
    try:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        credentials,_ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
        session = AuthorizedSession(credentials)
        snapshot = get_snapshot(session,project,now)
        response = session.get(f'https://compute.googleapis.com/compute/v1/projects/{project}/zones/europe-west3-c/instances/airflow-vm',timeout=8)
        response.raise_for_status()
        vm_status = response.json()['status']
        # A missing final manifest is not proof of an absent run; use the independent start log.
        day = now.astimezone(BERLIN).date().isoformat()
        response = session.post('https://logging.googleapis.com/v2/entries:list',json={
            'resourceNames':['projects/'+project],'pageSize':1,'orderBy':'timestamp desc',
            'filter':f'logName="projects/{project}/logs/cocoa-operations" jsonPayload.event="run_started" jsonPayload.business_date="{day}" NOT jsonPayload.engineering_validation=true'},timeout=8)
        response.raise_for_status()
        started = bool(response.json().get('entries'))
        subscriptions = json.loads(os.environ['WATCH_SUBSCRIPTIONS'])
        selector = ' OR '.join('resource.labels.subscription_id="'+name+'"' for name in subscriptions)
        response = session.get(f'https://monitoring.googleapis.com/v3/projects/{project}/timeSeries',params={
            'filter':'resource.type="pubsub_subscription" AND metric.type="pubsub.googleapis.com/subscription/oldest_unacked_message_age_by_region" AND ('+selector+')',
            'interval.startTime':(now-timedelta(minutes=10)).isoformat(),'interval.endTime':now.isoformat(),
            'pageSize':100,'view':'FULL'},timeout=8)
        response.raise_for_status()
        data = response.json()
        if data.get('nextPageToken'):
            raise ValueError('Queue metric result exceeded the bounded page')
        values = [float(s['points'][0]['value'].get('int64Value',0)) for s in data.get('timeSeries',[]) if s.get('points')]
        alerts = [] if probe else assess(now,snapshot,vm_status,started,max(values,default=0))
        for severity,code,message in alerts:
            emit(project,now,severity,code,message,snapshot)
        print(json.dumps({'severity':'INFO','event':'watchdog_complete','business_date':day,'alerts':len(alerts),
                          'delivery_status':snapshot.get('delivery_status'),'vm_status':vm_status,
                          'queue_series_sampled':len(values)}))
        return {'status':'probe_checked' if probe else 'checked','alerts':len(alerts)},200
    except Exception:
        emit(project,now,'ERROR','monitor_failed','Watchdog could not verify delivery; inspect its logs and permissions.')
        return {'status':'monitor_failed'},503
