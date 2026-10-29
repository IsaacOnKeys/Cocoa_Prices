"""Prepare guarded GCS/BigQuery fixtures for the native Airflow completion gate."""

import argparse
import hashlib
import json
import re
import runpy
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from validate_delivery import PROJECT, REGION, ROOT, api
from validate_ingestion import credentials

CONTRACT = {'cocoa': ('ICCO', ('cocoa_price',), None), 'oil': ('EIA', ('oil_price',), 'eia-oil-trigger'),
            'weather': ('NASA_POWER', ('precipitation', 'soil_moisture'), 'weather-trigger')}
LABELS = {'app': 'cocoa-prices', 'purpose': 'required-feed-validation'}
CASES = ('ready', 'delayed', 'missing', 'stale', 'prior')


def names(token):
    if not isinstance(token, str) or not re.fullmatch(r'20[0-9]{6}_[0-9a-f]{8}', token):
        raise ValueError('Invalid required-feed validation namespace')
    return {'dataset': PROJECT+'.required_feed_validation_'+token,
            'bucket': 'cocoa-required-feeds-'+token.replace('_', '-')}


def checked(path):
    state = json.loads(path.read_text())
    if state['names'] != names(state['token']) or set(state['runs']) != set(CASES):
        raise ValueError('Validation destinations were changed')
    for case, run in state['runs'].items():
        if run['run_id'] != 'required_feed_validation__'+state['token']+'__'+case:
            raise ValueError('Unexpected validation run identity')
    return state


def clients():
    from google.auth.transport.requests import AuthorizedSession
    from google.cloud import bigquery, storage

    credential = credentials()
    return AuthorizedSession(credential), storage.Client(project=PROJECT, credentials=credential), bigquery.Client(
        project=PROJECT, credentials=credential, location=REGION)


def prepare(path, business_date):
    from google.cloud import bigquery

    business = date.fromisoformat(business_date)
    if business.isoformat() != business_date or path.exists():
        raise ValueError('Expected a canonical business date and a new receipt file')
    now = datetime.now(timezone.utc)
    token = now.strftime('%Y%m%d')+'_'+uuid.uuid4().hex[:8]
    state = {'token': token, 'names': names(token), 'business_date': business_date, 'created_at': now.isoformat(),
             'runs': {case: {'run_id': 'required_feed_validation__'+token+'__'+case,
                             'start_date': now.isoformat(), 'invocation_started_at': (now+timedelta(seconds=1)).isoformat()}
                      for case in CASES}, 'phase': 'preparing', 'production_writes': False, 'load_jobs': {}}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2))
    http, gcs, bq = clients()
    api(http, 'POST', 'https://storage.googleapis.com/storage/v1/b', params={'project': PROJECT}, json={
        'name': state['names']['bucket'], 'location': REGION, 'labels': LABELS,
        'softDeletePolicy': {'retentionDurationSeconds': '0'},
        'iamConfiguration': {'uniformBucketLevelAccess': {'enabled': True}, 'publicAccessPrevention': 'enforced'}})
    dataset = bigquery.Dataset(state['names']['dataset'])
    dataset.location, dataset.labels = REGION, LABELS
    bq.create_dataset(dataset)
    policy = runpy.run_path(str(ROOT/'functions/shared/request_policy.py'))
    end, start = now.date(), now.date()-timedelta(days=30)
    for feed, (source, measurements, topic) in CONTRACT.items():
        rows = []
        for case, run in state['runs'].items():
            scope = {'service': 'pubsub.googleapis.com', 'name': f'projects/{PROJECT}/topics/{topic}',
                     'type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage'} if topic else None
            identity = policy['acquisition_identity'](source, key='cocoa_daily:'+run['run_id']+':'+feed,
                                                       context=SimpleNamespace(resource=scope))
            acquired = now+timedelta(seconds=2) if case != 'prior' else now-timedelta(seconds=30)
            observed = [start+timedelta(days=i) for i in range(31)] if feed == 'weather' else [end-timedelta(days=2), end-timedelta(days=1)]
            inventory = []
            for point in observed:
                values = {m: (5000.0 if feed == 'cocoa' else 70.0 if feed == 'oil' else 2.5 if m == 'precipitation' else 0.4) for m in measurements}
                if case == 'stale' and feed == 'weather' and point > business-timedelta(days=8):
                    values['soil_moisture'] = None
                observation = policy['observation_identity'](source, point.isoformat(), values)
                inventory.append({'date': point.isoformat(), 'observation_id': observation,
                                  'missing_measurements': sorted(m for m in measurements if values[m] is None)})
                row = {'date': point.isoformat(), **values, 'observation_id': observation, 'acquisition_id': identity,
                       'producer_acquired_at': acquired.isoformat(), 'consumer_received_at': (now+timedelta(seconds=3)).isoformat(),
                       'timestamp_status': 'valid'}
                if case == 'delayed' and feed == 'weather' and point == end:
                    state['delayed_row'] = row
                else:
                    rows.append(row)
            document = {'version': 1, 'stage': 'publish_acknowledged', 'source': source, 'status': 'published',
                        'published': len(inventory), 'acquisition_id': identity, 'acquired_at': acquired.isoformat(),
                        'window_start': start.isoformat(), 'window_end': end.isoformat(), 'observations': inventory,
                        'observation_start': observed[0].isoformat(), 'observation_end': observed[-1].isoformat(),
                        'not_exposed_dates': sorted({(start+timedelta(days=i)).isoformat() for i in range(31)}-{r['date'] for r in inventory})}
            if not (case == 'missing' and feed == 'weather'):
                encoded = json.dumps(document, sort_keys=True, separators=(',', ':')).encode()
                key = f'acquisition-coverage/v1/{source}/receipts/{identity}/'+hashlib.sha256(encoded).hexdigest()+'.json'
                gcs.bucket(state['names']['bucket']).blob(key).upload_from_string(encoded, content_type='application/json', if_generation_match=0, timeout=30)
        schema = [bigquery.SchemaField('date', 'DATE'), *[bigquery.SchemaField(m, 'FLOAT64') for m in measurements],
                  *[bigquery.SchemaField(m, 'STRING') for m in ('observation_id', 'acquisition_id', 'timestamp_status')],
                  *[bigquery.SchemaField(m, 'TIMESTAMP') for m in ('producer_acquired_at', 'consumer_received_at')]]
        job = bq.load_table_from_json(rows, state['names']['dataset']+'.'+feed,
                                      job_config=bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_EMPTY'))
        state['load_jobs'][feed] = job.job_id
        path.write_text(json.dumps(state, indent=2))
        job.result(timeout=120)
    state['phase'] = 'ready'
    path.write_text(json.dumps(state, indent=2))
    print(json.dumps({'phase': 'ready', 'token': token, 'cases': list(CASES), 'production_writes': False}))


def advance(path):
    from google.cloud import bigquery

    state = checked(path)
    if state.get('delayed_row_added'):
        raise ValueError('Delayed validation row was already added')
    _, _, bq = clients()
    job = bq.load_table_from_json([state['delayed_row']], state['names']['dataset']+'.weather',
                                 job_config=bigquery.LoadJobConfig(write_disposition='WRITE_APPEND'))
    state['delayed_load_job'] = job.job_id
    path.write_text(json.dumps(state, indent=2))
    job.result(timeout=120)
    state['delayed_row_added'] = True
    path.write_text(json.dumps(state, indent=2))
    print(json.dumps({'delayed_row_added': True, 'production_writes': False}))


def cleanup(path):
    from google.api_core.exceptions import NotFound

    state = checked(path)
    _, gcs, bq = clients()
    try:
        dataset = bq.get_dataset(state['names']['dataset'])
    except NotFound:
        dataset = None
    if dataset:
        if dataset.labels.get('purpose') != LABELS['purpose']:
            raise ValueError('Refusing to delete an unlabelled dataset')
        bq.delete_dataset(dataset, delete_contents=True, not_found_ok=True)
    bucket = gcs.bucket(state['names']['bucket'])
    if bucket.exists(timeout=10):
        bucket.reload(timeout=10)
        if bucket.labels.get('purpose') != LABELS['purpose']:
            raise ValueError('Refusing to delete an unlabelled bucket')
        for blob in bucket.list_blobs(versions=True, timeout=10):
            if not blob.name.startswith('acquisition-coverage/v1/'):
                raise ValueError('Unexpected validation object')
            blob.delete(if_generation_match=blob.generation, timeout=10)
        bucket.delete(timeout=30)
    assert not bucket.exists(timeout=10)
    state['resources_deleted'] = True
    path.write_text(json.dumps(state, indent=2))
    print(json.dumps({'resources_deleted': True, 'token': state['token']}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'advance', 'cleanup'))
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--business-date')
    args = parser.parse_args()
    if args.action == 'prepare':
        prepare(args.state, args.business_date)
    else:
        globals()[args.action](args.state)
