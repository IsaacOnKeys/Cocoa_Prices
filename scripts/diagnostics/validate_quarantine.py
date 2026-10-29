"""Validate exact consumer bytes with disposable authenticated push, GCS and BigQuery."""

import argparse
import base64
import hashlib
import io
import json
import re
import runpy
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import fastavro

from validate_delivery import ACCOUNT, NUMBER, PROJECT, REGION, ROOT, api, clients, grant, operation
from validate_ingestion import credentials, MEASUREMENTS, TABLES

RUNTIME = f'analytics-staging@{PROJECT}.iam.gserviceaccount.com'
PUSH = f'analytics-push@{PROJECT}.iam.gserviceaccount.com'
LABELS = {'app': 'cocoa-prices', 'purpose': 'quarantine-validation'}


def names(token):
    if not isinstance(token, str) or not re.fullmatch(r'20[0-9]{6}_[0-9a-f]{8}', token):
        raise ValueError('Invalid quarantine validation namespace')
    base = 'quarantine-validation-'+token.replace('_', '-')
    return {'function': base, 'bucket': 'cocoa-quarantine-'+token.replace('_', '-'),
            'dataset': PROJECT+'.quarantine_validation_'+token,
            'topics': {feed: base+'-'+feed for feed in (*TABLES, 'sink')},
            'subscriptions': {feed: base+'-'+feed+'-sub' for feed in (*TABLES, 'sink')}}


def checked(path):
    state = json.loads(path.read_text())
    if state['names'] != names(state['token']):
        raise ValueError('Quarantine destinations were changed')
    src = state.get('source')
    if src and (src['bucket'] != f'gcf-v2-sources-{NUMBER}-{REGION}' or not re.fullmatch(r'[0-9a-f]{64}', src['sha256'])
                or src['object'] != state['names']['function']+'/source-'+src['sha256']+'.zip' or not str(src['generation']).isdigit()):
        raise ValueError('Unexpected validation source')
    return state


def save(path, state):
    path.write_text(json.dumps(state, indent=2))


def storage_access(bucket, creator):
    policy = bucket.get_iam_policy(requested_policy_version=3, timeout=30)
    member = 'serviceAccount:'+RUNTIME
    for binding in list(policy.bindings):
        if member in binding['members']:
            binding['members'].remove(member)
            if not binding['members']:
                policy.bindings.remove(binding)
    for role in ['roles/storage.objectViewer']+(['roles/storage.objectCreator'] if creator else []):
        policy.bindings.append({'role': role, 'members': {member}, 'condition': {
            'title': 'ingestion_quarantine_prefix', 'expression': f"resource.name.startsWith('projects/_/buckets/{bucket.name}/objects/quarantine/v1/')"}})
    bucket.set_iam_policy(policy, timeout=30)


def prepare(path):
    from google.cloud import bigquery

    if path.exists():
        raise ValueError('Refusing to overwrite a validation receipt')
    token = datetime.now(timezone.utc).strftime('%Y%m%d')+'_'+uuid.uuid4().hex[:8]
    state = {'token': token, 'names': names(token), 'phase': 'preparing', 'production_writes': False, 'cases': {}}
    path.parent.mkdir(parents=True, exist_ok=True)
    save(path, state)
    n = state['names']
    http, gcs, publisher, _ = clients()
    api(http, 'POST', 'https://storage.googleapis.com/storage/v1/b', params={'project': PROJECT}, json={
        'name': n['bucket'], 'location': REGION, 'labels': LABELS, 'versioning': {'enabled': True},
        'softDeletePolicy': {'retentionDurationSeconds': '0'},
        'iamConfiguration': {'uniformBucketLevelAccess': {'enabled': True}, 'publicAccessPrevention': 'enforced'},
        'lifecycle': {'rule': [
            {'action': {'type': 'Delete'}, 'condition': {'age': 400, 'isLive': True, 'matchesPrefix': ['daily-inputs/v1/', 'quarantine/v1/']}},
            {'action': {'type': 'Delete'}, 'condition': {'age': 400, 'isLive': False, 'daysSinceNoncurrentTime': 30, 'matchesPrefix': ['daily-inputs/v1/', 'quarantine/v1/']}}]}})
    storage_access(gcs.bucket(n['bucket']), False)
    bq = bigquery.Client(project=PROJECT, credentials=credentials(), location=REGION)
    dataset = bigquery.Dataset(n['dataset'])
    dataset.location, dataset.labels, dataset.default_table_expiration_ms = REGION, {**LABELS, 'token': token}, 86_400_000
    bq.create_dataset(dataset)
    for feed, table in TABLES.items():
        fields = bq.get_table(PROJECT+'.stream_staging.'+table).schema
        bq.create_table(bigquery.Table(n['dataset']+'.'+table, schema=fields))
        if feed == 'oil':
            fields = [bigquery.SchemaField(f.name, 'BOOL', mode=f.mode) if f.name == 'oil_price' else f for f in fields]
            bq.create_table(bigquery.Table(n['dataset']+'.invalid_oil_schema', schema=fields))
    for topic in n['topics'].values():
        publisher.create_topic(request={'name': f'projects/{PROJECT}/topics/{topic}', 'labels': LABELS}, timeout=30, retry=None)
    package = runpy.run_path(str(ROOT/'scripts/deploy/package_functions.py'))['package_function']
    runtime, archive = package('ingestion', output_dir=path.parent/('packages-'+token))
    data = archive.read_bytes()
    checksum = hashlib.sha256(data).hexdigest()
    blob = gcs.bucket(f'gcf-v2-sources-{NUMBER}-{REGION}').blob(n['function']+'/source-'+checksum+'.zip')
    blob.upload_from_string(data, content_type='application/zip', if_generation_match=0, timeout=30)
    state['source'] = {'bucket': blob.bucket.name, 'object': blob.name, 'generation': str(blob.generation), 'sha256': checksum}
    state['members_sha256'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in runtime.iterdir()}
    routes = {f'projects/{PROJECT}/subscriptions/{sub}': {'feed': 'oil' if feed == 'sink' else feed,
              'table': n['dataset']+'.'+('invalid_oil_schema' if feed == 'sink' else TABLES[feed])} for feed, sub in n['subscriptions'].items()}
    save(path, state)
    result = api(http, 'POST', f'https://cloudfunctions.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/functions',
                 params={'functionId': n['function']}, json={
                     'name': f'projects/{PROJECT}/locations/{REGION}/functions/{n["function"]}', 'labels': LABELS,
                     'buildConfig': {'runtime': 'python311', 'entryPoint': 'stage_analytics_data',
                         'source': {'storageSource': {k: state['source'][k] for k in ('bucket', 'object', 'generation')}},
                         'serviceAccount': f'projects/{PROJECT}/serviceAccounts/{ACCOUNT}'},
                     'serviceConfig': {'serviceAccountEmail': RUNTIME, 'availableMemory': '512M', 'availableCpu': '1',
                         'maxInstanceCount': 3, 'maxInstanceRequestConcurrency': 1, 'timeoutSeconds': 60,
                         'environmentVariables': {'GCP_PROJECT': PROJECT, 'INGESTION_ROUTES': json.dumps(routes),
                             'INGESTION_QUARANTINE_BUCKET': n['bucket'], 'LOG_EXECUTION_ID': 'true'}}})
    state['create_operation'] = result['name']
    save(path, state)
    operation(http, result)
    meta = api(http, 'GET', f'https://cloudfunctions.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/functions/{n["function"]}')
    assert meta['state'] == 'ACTIVE' and meta['buildConfig']['runtime'] == 'python311'
    src = meta['buildConfig'].get('sourceProvenance', {}).get('resolvedStorageSource') or meta['buildConfig']['source']['storageSource']
    deployed = gcs.bucket(src['bucket']).blob(src['object'], generation=int(src['generation'])).download_as_bytes(timeout=30)
    assert hashlib.sha256(deployed).hexdigest() == checksum
    grant(http, 'https://run.googleapis.com/v2/'+meta['serviceConfig']['service'], 'serviceAccount:'+PUSH, 'roles/run.invoker')
    from google.cloud import pubsub_v1

    subscriber = pubsub_v1.SubscriberClient(credentials=credentials())
    for feed, sub in n['subscriptions'].items():
        subscriber.create_subscription(request={'name': f'projects/{PROJECT}/subscriptions/{sub}',
            'topic': f'projects/{PROJECT}/topics/{n["topics"][feed]}', 'labels': LABELS,
            'ack_deadline_seconds': 60, 'message_retention_duration': {'seconds': 86400},
            'retry_policy': {'minimum_backoff': {'seconds': 10}, 'maximum_backoff': {'seconds': 30}},
            'push_config': {'push_endpoint': meta['serviceConfig']['uri'], 'oidc_token': {'service_account_email': PUSH, 'audience': meta['serviceConfig']['uri']}}}, timeout=30, retry=None)
    state.update(phase='ready', revision=meta['serviceConfig']['revision'])
    save(path, state)
    print(json.dumps({'phase': 'ready', 'token': token, 'exact_candidate_zip_sha256': checksum}), flush=True)


def exercise(path):
    from google.cloud import bigquery

    state = checked(path)
    assert state['phase'] == 'ready' and not state.get('resources_deleted')
    n = state['names']
    http, gcs, publisher, _ = clients()
    bucket = gcs.bucket(n['bucket'])
    bq = bigquery.Client(project=PROJECT, credentials=credentials(), location=REGION)

    def publish(case, feed, changes=None, malformed=False, destination=None):
        if case in state['cases']:
            return state['cases'][case]
        observed = (datetime.now(timezone.utc).date()-timedelta(days=1)).isoformat()
        record = {'date': observed, **MEASUREMENTS[feed], 'ingestion_time': datetime.now(timezone.utc).isoformat(),
                  'raw_payload': json.dumps({'validation': state['token'], 'case': case}), **(changes or {})}
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, json.loads((ROOT/'schemas'/f'{feed}_schema.avsc').read_bytes()), record)
        data = b'\xff' if malformed else buffer.getvalue()
        identity = hashlib.sha256((state['token']+':'+case).encode()).hexdigest()
        key = destination or feed
        message = publisher.publish(f'projects/{PROJECT}/topics/{n["topics"][key]}', data,
            googclient_schemaencoding='BINARY', acquisition_id=identity, observation_id=hashlib.sha256(data).hexdigest()).result(timeout=30)
        sub = f'projects/{PROJECT}/subscriptions/{n["subscriptions"][key]}'
        result = {'feed': feed, 'message_id': message, 'acquisition_id': identity,
                  'delivery_id': hashlib.sha256((sub+'\0'+message).encode()).hexdigest(),
                  'wire_sha256': hashlib.sha256(data).hexdigest(), 'subscription': sub}
        state['cases'][case] = result
        save(path, state)
        return result

    def wait_retry(case, evidence):
        deadline = time.monotonic()+150
        while time.monotonic() < deadline:
            result = api(http, 'POST', 'https://logging.googleapis.com/v2/entries:list', json={
                'resourceNames': ['projects/'+PROJECT], 'filter': f'resource.type="cloud_run_revision" AND resource.labels.service_name="{n["function"]}" '
                    f'AND jsonPayload.event="ingestion_retry" AND jsonPayload.delivery_id="{case["delivery_id"]}" AND jsonPayload.evidence_persisted={str(evidence).lower()}',
                'pageSize': 10, 'orderBy': 'timestamp desc'})
            if result.get('entries'):
                return result['entries'][0]['jsonPayload']
            time.sleep(5)
        raise RuntimeError('Expected native retry was not observed; retain resources and inspect bounded logs')

    denied = publish('quarantine_denied_then_recovered', 'weather', malformed=True)
    state['quarantine_denied_retry'] = wait_retry(denied, False)
    save(path, state)
    storage_access(bucket, True)
    transient = publish('sink_denied_then_recovered', 'oil')
    state['sink_denied_retry'] = wait_retry(transient, True)
    save(path, state)
    dataset = bq.get_dataset(n['dataset'])
    if not any(entry.entity_id == RUNTIME and entry.role == 'WRITER' for entry in dataset.access_entries):
        dataset.access_entries = [*dataset.access_entries, bigquery.AccessEntry('WRITER', 'userByEmail', RUNTIME)]
        bq.update_dataset(dataset, ['access_entries'])
    for feed in TABLES:
        publish('healthy_'+feed, feed)
        publish('malformed_'+feed, feed, malformed=True)
        changes = {'soil_moisture': 1.5} if feed == 'weather' else {'date': None}
        publish('business_'+feed, feed, changes=changes)
    publish('permanent_sink_rejection', 'oil', destination='sink')
    expected_invalid = {c['acquisition_id'] for name, c in state['cases'].items() if name not in ('sink_denied_then_recovered', 'healthy_cocoa', 'healthy_oil', 'healthy_weather')}
    expected_valid = {c['acquisition_id'] for name, c in state['cases'].items() if name.startswith('healthy_') or name == 'sink_denied_then_recovered'}
    deadline = time.monotonic()+180
    while time.monotonic() < deadline:
        documents = []
        for blob in bucket.list_blobs(prefix='quarantine/v1/', timeout=30):
            data = blob.download_as_bytes(if_generation_match=blob.generation, timeout=10)
            assert blob.name.endswith('/'+hashlib.sha256(data).hexdigest()+'.json')
            document = json.loads(data)
            original = base64.b64decode(document['envelope_base64'], validate=True)
            assert hashlib.sha256(original).hexdigest() == document['input_sha256'] and document['envelope_complete']
            body = json.loads(original)
            case = next(c for c in state['cases'].values() if c['acquisition_id'] == document['acquisition_id'])
            assert body['message']['messageId'] == case['message_id'] and document['pubsub_message_id'] == case['message_id']
            assert hashlib.sha256(base64.b64decode(body['message']['data'])).hexdigest() == case['wire_sha256']
            documents.append(document)
        quarantined = {d['acquisition_id'] for d in documents if d['disposition'] == 'quarantined'}
        accepted = set()
        for table in TABLES.values():
            query = bq.query(f'SELECT DISTINCT acquisition_id FROM `{n["dataset"]}.{table}`', job_config=bigquery.QueryJobConfig(maximum_bytes_billed=1073741824))
            accepted.update(r['acquisition_id'] for r in query.result(timeout=30))
        if quarantined == expected_invalid and accepted == expected_valid:
            break
        time.sleep(5)
    else:
        raise RuntimeError('Native quarantine accounting did not reconcile; retain isolated resources')
    assert not quarantined & accepted
    sink = next(d for d in documents if d['acquisition_id'] == state['cases']['permanent_sink_rejection']['acquisition_id'] and d['disposition'] == 'quarantined')
    assert sink['error_category'] == 'sink' and sink['error_details']['reason_codes'] == ['invalid']
    state.update(phase='passed', accounted_cases=len(state['cases']), accepted=len(accepted), quarantined=len(quarantined),
                 exact_envelopes_and_checksums_verified=True, scoped_storage_permissions_verified=True,
                 sink_retry_and_recovery_verified=True, quarantine_retry_and_recovery_verified=True)
    save(path, state)
    print(json.dumps({k: state[k] for k in ('phase', 'token', 'accounted_cases', 'accepted', 'quarantined', 'sink_retry_and_recovery_verified', 'quarantine_retry_and_recovery_verified')}))


def cleanup(path):
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    state = checked(path)
    n = state['names']
    http, gcs, _, _ = clients()
    for kind, values in [('subscriptions', n['subscriptions'].values()), ('topics', n['topics'].values())]:
        for name in values:
            url = f'https://pubsub.googleapis.com/v1/projects/{PROJECT}/{kind}/{name}'
            meta = api(http, 'GET', url, allow_missing=True)
            if meta:
                if meta.get('labels', {}).get('purpose') != LABELS['purpose']:
                    raise ValueError('Refusing to delete a non-validation transport')
                api(http, 'DELETE', url)
    url = f'https://cloudfunctions.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/functions/{n["function"]}'
    meta = api(http, 'GET', url, allow_missing=True)
    if meta:
        if meta.get('labels', {}).get('purpose') != LABELS['purpose']:
            raise ValueError('Refusing to delete a non-validation function')
        operation(http, api(http, 'DELETE', url))
    bucket = gcs.bucket(n['bucket'])
    if bucket.exists(timeout=10):
        bucket.reload(timeout=10)
        if bucket.labels.get('purpose') != LABELS['purpose']:
            raise ValueError('Refusing to delete a non-validation bucket')
        for blob in bucket.list_blobs(versions=True, timeout=30):
            if not blob.name.startswith('quarantine/v1/'):
                raise ValueError('Unexpected object in validation bucket')
            blob.delete(if_generation_match=blob.generation, timeout=10)
        bucket.delete(timeout=30)
    bq = bigquery.Client(project=PROJECT, credentials=credentials(), location=REGION)
    try:
        dataset = bq.get_dataset(n['dataset'])
    except NotFound:
        dataset = None
    if dataset:
        if dataset.labels.get('purpose') != LABELS['purpose'] or dataset.labels.get('token') != state['token']:
            raise ValueError('Refusing to delete a non-validation dataset')
        bq.delete_dataset(dataset, delete_contents=True)
    if state.get('source'):
        src = state['source']
        blob = gcs.bucket(src['bucket']).blob(src['object'], generation=int(src['generation']))
        if blob.exists(timeout=10):
            blob.delete(if_generation_match=int(src['generation']), timeout=10)
    assert api(http, 'GET', url, allow_missing=True) is None and not bucket.exists(timeout=10)
    state['resources_deleted'] = True
    save(path, state)
    print(json.dumps({'resources_deleted': True, 'token': state['token']}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'exercise', 'cleanup'))
    parser.add_argument('--state', type=Path, required=True)
    args = parser.parse_args()
    globals()[args.action](args.state)
