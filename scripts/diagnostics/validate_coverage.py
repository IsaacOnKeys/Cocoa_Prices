"""Guarded live-source/outage/revision validation in disposable managed resources."""

import argparse
import base64
import contextlib
import hashlib
import io
import json
import re
import runpy
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZIP_STORED, ZipFile

from validate_delivery import ACCOUNT, NUMBER, PROJECT, REGION, ROOT, api, clients, grant, operation, topology

SOURCES = {'cocoa': 'ICCO', 'oil': 'EIA', 'weather': 'NASA_POWER'}
LABELS = {'app': 'cocoa-prices', 'purpose': 'coverage-validation'}


def names(token):
    if not isinstance(token, str) or not re.fullmatch(r'20[0-9]{6}_[0-9a-f]{8}', token):
        raise ValueError('Invalid coverage-validation namespace')
    base = 'coverage-validation-' + token.replace('_', '-')
    return {'function': base, 'trigger': base+'-trigger',
            'topics': {feed: base+'-'+feed for feed in SOURCES},
            'subscriptions': {feed: base+'-'+feed+'-sub' for feed in SOURCES},
            'buckets': {kind: 'cocoa-coverage-'+token.replace('_', '-')+'-'+kind for kind in ('receipts', 'state')}}


def checked(path):
    state = json.loads(path.read_text())
    if state['names'] != names(state['token']):
        raise ValueError('Validation resource names were changed')
    if state.get('replay_dataset') and state['replay_dataset'] != PROJECT+'.raw_replay_validation_'+state['token']:
        raise ValueError('Unexpected replay validation dataset')
    for src in [*state.get('prior_sources', []), *([state['source']] if state.get('source') else [])]:
        if (src['bucket'] != f'gcf-v2-sources-{NUMBER}-{REGION}'
                or not re.fullmatch(r'[0-9a-f]{64}', src['sha256'])
                or src['object'] != state['names']['function']+'/probe-'+src['sha256']+'.zip'
                or not str(src['generation']).isdecimal()):
            raise ValueError('Unexpected validation source')
    return state


def save(path, state):
    path.write_text(json.dumps(state, indent=2))


def source_inputs():
    contents = {'main.py': (ROOT/'scripts/diagnostics/coverage_probe.py').read_bytes(),
                'request_policy.py': (ROOT/'functions/shared/request_policy.py').read_bytes()}
    requirements = set()
    for feed in SOURCES:
        contents[feed+'_producer.py'] = (ROOT/f'functions/{feed}/main.py').read_bytes()
        contents[feed+'_schema.avsc'] = (ROOT/f'schemas/{feed}_schema.avsc').read_bytes()
        requirements.update((ROOT/f'functions/{feed}/requirements.txt').read_text().splitlines())
    contents['requirements.txt'] = ('\n'.join(sorted(requirements))+'\n').encode()
    return contents


def prepare(path):
    if path.exists():
        raise ValueError('Refusing to overwrite a validation receipt')
    token = datetime.now(timezone.utc).strftime('%Y%m%d')+'_'+uuid.uuid4().hex[:8]
    state = {'token': token, 'names': names(token), 'phase': 'preparing', 'production_writes': False}
    path.parent.mkdir(parents=True, exist_ok=True)
    save(path, state)
    n = state['names']
    http, gcs, publisher, subscriber = clients()
    for kind, name in n['buckets'].items():
        api(http, 'POST', 'https://storage.googleapis.com/storage/v1/b', params={'project': PROJECT}, json={
            'name': name, 'location': REGION, 'labels': LABELS, 'softDeletePolicy': {'retentionDurationSeconds': '0'},
            **({'versioning': {'enabled': True}, 'lifecycle': {'rule': [
                {'action': {'type': 'Delete'}, 'condition': {'age': 400, 'isLive': True, 'matchesPrefix': ['daily-inputs/v1/']}},
                {'action': {'type': 'Delete'}, 'condition': {'age': 400, 'isLive': False, 'daysSinceNoncurrentTime': 30, 'matchesPrefix': ['daily-inputs/v1/']}}
            ]}} if kind == 'receipts' else {}),
            'iamConfiguration': {'uniformBucketLevelAccess': {'enabled': True}, 'publicAccessPrevention': 'enforced'}})
        bucket = gcs.bucket(name)
        policy = bucket.get_iam_policy(requested_policy_version=3, timeout=30)
        roles = ('roles/storage.objectCreator', 'roles/storage.objectViewer') if kind == 'receipts' else ('roles/storage.objectAdmin',)
        for role in roles:
            policy.bindings.append({'role': role, 'members': {'serviceAccount:'+ACCOUNT}})
        bucket.set_iam_policy(policy, timeout=30)
    today = datetime.now(timezone.utc).date()
    for source in SOURCES.values():
        seed = {'version': 1, 'source': source, 'stage': 'publish_acknowledged',
                'window_start': (today-timedelta(days=40)).isoformat(), 'window_end': (today-timedelta(days=5)).isoformat(),
                'acquired_at': (datetime.now(timezone.utc)-timedelta(days=5)).isoformat(),
                'receipt_object': 'isolated-simulated-pre-outage-seed'}
        gcs.bucket(n['buckets']['state']).blob(f'acquisition-coverage/v1/{source}/state.json').upload_from_string(
            json.dumps(seed), if_generation_match=0, timeout=30)
    state['simulated_previous_frontier'] = (today-timedelta(days=5)).isoformat()
    for topic in [n['trigger'], *n['topics'].values()]:
        publisher.create_topic(request={'name': f'projects/{PROJECT}/topics/{topic}', 'labels': LABELS}, timeout=30, retry=None)
    for feed, sub in n['subscriptions'].items():
        subscriber.create_subscription(request={'name': f'projects/{PROJECT}/subscriptions/{sub}',
            'topic': f'projects/{PROJECT}/topics/{n["topics"][feed]}', 'ack_deadline_seconds': 60,
            'message_retention_duration': {'seconds': 86400}, 'expiration_policy': {}, 'labels': LABELS}, timeout=30, retry=None)
    contents = source_inputs()
    state['members_sha256'] = {name: hashlib.sha256(value).hexdigest() for name, value in contents.items()}
    stream = io.BytesIO()
    with ZipFile(stream, 'w', compression=ZIP_STORED) as bundle:
        for name, value in sorted(contents.items()):
            bundle.writestr(name, value)
    payload = stream.getvalue()
    digest = hashlib.sha256(payload).hexdigest()
    blob = gcs.bucket(f'gcf-v2-sources-{NUMBER}-{REGION}').blob(n['function']+'/probe-'+digest+'.zip')
    blob.upload_from_string(payload, content_type='application/zip', if_generation_match=0, timeout=30)
    state['source'] = {'bucket': blob.bucket.name, 'object': blob.name, 'generation': str(blob.generation), 'sha256': digest}
    save(path, state)
    document = api(http, 'POST', f'https://cloudfunctions.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/functions',
        params={'functionId': n['function']}, json={
            'name': f'projects/{PROJECT}/locations/{REGION}/functions/{n["function"]}', 'labels': LABELS,
            'buildConfig': {'runtime': 'python311', 'entryPoint': 'validate_coverage',
                'source': {'storageSource': {k: state['source'][k] for k in ('bucket', 'object', 'generation')}},
                'serviceAccount': f'projects/{PROJECT}/serviceAccounts/{ACCOUNT}'},
            'serviceConfig': {'maxInstanceCount': 1, 'minInstanceCount': 0, 'maxInstanceRequestConcurrency': 1,
                'availableMemory': '512M', 'availableCpu': '1', 'timeoutSeconds': 60, 'serviceAccountEmail': ACCOUNT,
                'environmentVariables': {'PROBE_TOKEN': token, 'COVERAGE_RECEIPT_BUCKET': n['buckets']['receipts'],
                    'COVERAGE_STATE_BUCKET': n['buckets']['state'], 'COCOA_TOPIC': n['topics']['cocoa'],
                    'OIL_TOPIC': n['topics']['oil'], 'WEATHER_TOPIC': n['topics']['weather']}},
            'eventTrigger': {'triggerRegion': REGION, 'eventType': 'google.cloud.pubsub.topic.v1.messagePublished',
                'pubsubTopic': f'projects/{PROJECT}/topics/{n["trigger"]}', 'serviceAccountEmail': ACCOUNT,
                'retryPolicy': 'RETRY_POLICY_DO_NOT_RETRY'}})
    state['create_operation'] = document['name']
    save(path, state)
    operation(http, document)
    live = topology(http, n['function'])
    grant(http, 'https://run.googleapis.com/v2/'+live['function']['serviceConfig']['service'],
          'serviceAccount:'+ACCOUNT, 'roles/run.invoker')
    state.update(phase='ready', transport=live['subscription']['name'])
    save(path, state)
    print(json.dumps({'phase': 'ready', 'token': token}), flush=True)


def refresh(path):
    """Rebuild an unexercised private probe after a fixture or source correction."""
    state = checked(path)
    if state['phase'] != 'ready' or state.get('cases'):
        raise ValueError('Only an unexercised ready probe can be rebuilt')
    http, gcs, _, _ = clients()
    before = topology(http, state['names']['function'])
    assert before['function']['labels']['purpose'] == LABELS['purpose']
    contents = source_inputs()
    stream = io.BytesIO()
    with ZipFile(stream, 'w', compression=ZIP_STORED) as bundle:
        for name, value in sorted(contents.items()):
            bundle.writestr(name, value)
    payload = stream.getvalue()
    digest = hashlib.sha256(payload).hexdigest()
    blob = gcs.bucket(state['source']['bucket']).blob(state['names']['function']+'/probe-'+digest+'.zip')
    blob.upload_from_string(payload, content_type='application/zip', if_generation_match=0, timeout=30)
    state.setdefault('prior_sources', []).append(state['source'])
    state['source'] = {'bucket': blob.bucket.name, 'object': blob.name, 'generation': str(blob.generation), 'sha256': digest}
    state['members_sha256'] = {name: hashlib.sha256(value).hexdigest() for name, value in contents.items()}
    save(path, state)
    document = api(http, 'PATCH', 'https://cloudfunctions.googleapis.com/v2/'+before['function']['name'],
        params={'updateMask': 'build_config.source'},
        json={'buildConfig': {'source': {'storageSource': {k: state['source'][k] for k in ('bucket', 'object', 'generation')}}}})
    state['refresh_operation'] = document['name']
    save(path, state)
    operation(http, document)
    after = topology(http, state['names']['function'])
    assert after['subscription']['name'] == before['subscription']['name']
    assert after['function']['serviceConfig']['environmentVariables'] == before['function']['serviceConfig']['environmentVariables']
    print(json.dumps({'phase': 'ready', 'refreshed': True, 'token': state['token']}), flush=True)


def exercise(path):
    import fastavro
    from google.api_core.exceptions import DeadlineExceeded

    state = checked(path)
    n = state['names']
    http, gcs, publisher, subscriber = clients()
    live = topology(http, n['function'])
    assert live['function']['labels']['purpose'] == LABELS['purpose']
    src = live['function']['buildConfig'].get('sourceProvenance', {}).get('resolvedStorageSource') or live['function']['buildConfig']['source']['storageSource']
    payload = gcs.bucket(src['bucket']).blob(src['object'], generation=int(src['generation'])).download_as_bytes(timeout=30)
    assert hashlib.sha256(payload).hexdigest() == state['source']['sha256']
    with ZipFile(io.BytesIO(payload)) as bundle:
        assert {name: hashlib.sha256(bundle.read(name)).hexdigest() for name in bundle.namelist()} == state['members_sha256']
    assert {name: hashlib.sha256(value).hexdigest() for name, value in source_inputs().items()} == state['members_sha256'], 'Probe source must match the current implementation'
    state.setdefault('cases', {})
    for feed in SOURCES:
        results = state['cases'].setdefault(feed, {})
        for case in ('live', 'fixture_initial', 'source_failure', 'partial_publish', 'fixture_corrected'):
            if case not in results:
                publisher.publish(f'projects/{PROJECT}/topics/{n["trigger"]}', json.dumps({'feed': feed, 'case': case}).encode()).result(timeout=30)
                deadline = time.monotonic()+120
                blob = gcs.bucket(n['buckets']['state']).blob(f'probe-results/{feed}/{case}.json')
                while not blob.exists(timeout=10):
                    if time.monotonic() > deadline:
                        raise RuntimeError('Coverage probe timed out; retain resources and inspect logs')
                    time.sleep(2)
                results[case] = json.loads(blob.download_as_bytes(timeout=10))
                save(path, state)
            record = results[case]
            expected = case not in ('source_failure', 'partial_publish')
            assert record['accepted'] is expected, feed+' '+case+' failed; inspect private receipt'
            if expected:
                receipt = record['receipt']
                assert receipt['published'] == len(receipt['observations']) > 0
                assert receipt['stage'] == 'publish_acknowledged'
                if case == 'live':
                    assert receipt['window_start'] == (datetime.now(timezone.utc).date()-timedelta(days=35)).isoformat()
            else:
                assert record['before'] == record['after'], 'Failed batch advanced coverage'
                if case == 'partial_publish':
                    assert record['controlled_publish_attempts'] == 2, 'Probe must fail after the first acknowledged publication'
            print(json.dumps({'feed': feed, 'case': case, 'accepted': expected}), flush=True)
        initial, corrected = results['fixture_initial']['receipt'], results['fixture_corrected']['receipt']
        assert initial['acquisition_id'] == corrected['acquisition_id']
        target = results['fixture_corrected']['target_date']
        first = next(r for r in initial['observations'] if r['date'] == target)
        second = next(r for r in corrected['observations'] if r['date'] == target)
        assert first['observation_id'] != second['observation_id']
        schema = fastavro.parse_schema(json.loads((ROOT/f'schemas/{feed}_schema.avsc').read_bytes()))
        expected_ids = {(r['acquisition_id'], item['observation_id'])
                        for case, result in results.items() if result['accepted']
                        for r in [result['receipt']] for item in r['observations']}
        ledger = state.setdefault('deliveries', {}).setdefault(feed, [])
        observed = {(r['acquisition_id'], r['observation_id']) for r in ledger}
        policy = runpy.run_path(str(ROOT/'functions/shared/request_policy.py'))
        count, deadline = len(ledger), time.monotonic()+90
        while (not expected_ids <= observed or not observed-expected_ids) and time.monotonic() < deadline:
            try:
                batch = subscriber.pull(request={'subscription': f'projects/{PROJECT}/subscriptions/{n["subscriptions"][feed]}',
                                                 'max_messages': 1000}, timeout=5, retry=None)
            except DeadlineExceeded:
                continue
            for received in batch.received_messages:
                row = fastavro.schemaless_reader(io.BytesIO(received.message.data), schema)
                values = {k: v for k, v in row.items() if k not in ('date', 'ingestion_time', 'raw_payload')}
                assert policy['observation_identity'](SOURCES[feed], row['date'], values) == received.message.attributes['observation_id']
                observed.add((received.message.attributes['acquisition_id'], received.message.attributes['observation_id']))
                ledger.append({'acquisition_id': received.message.attributes['acquisition_id'],
                               'observation_id': received.message.attributes['observation_id'],
                               'message_id': received.message.message_id, 'date': row['date']})
                count += 1
            if batch.received_messages:
                save(path, state)  # Persist validation evidence before ACK; reruns can resume.
                subscriber.acknowledge(request={'subscription': f'projects/{PROJECT}/subscriptions/{n["subscriptions"][feed]}',
                    'ack_ids': [r.ack_id for r in batch.received_messages]}, timeout=10, retry=None)
        assert expected_ids <= observed, 'Published inventory has unexplained delivery gaps'
        partial = observed - expected_ids
        assert len(partial) == 1, 'Exactly one value revision should remain from the controlled partial batch'
        state.setdefault('reconciliation', {})[feed] = {'expected_revisions': len(expected_ids), 'received_rows': count,
            'all_inventory_revisions_received': True, 'controlled_partial_revisions': len(partial), 'corrected_date': target}
        save(path, state)
    state.update(phase='passed', live_sources_passed=True, outage_recovery_passed=True, older_corrections_passed=True,
                 failed_batches_preserved_coverage=True, serving_members_match=True)
    save(path, state)
    print(json.dumps({'phase': 'passed', 'reconciliation': state['reconciliation']}), flush=True)


def cleanup(path):
    state = checked(path)
    n = state['names']
    http, gcs, _, _ = clients()
    if state.get('replay_dataset'):
        from google.cloud import bigquery
        from google.api_core.exceptions import NotFound
        from validate_ingestion import credentials

        dataset_id = PROJECT+'.raw_replay_validation_'+state['token']
        if state['replay_dataset'] != dataset_id:
            raise ValueError('Unexpected replay validation dataset')
        bq = bigquery.Client(project=PROJECT, credentials=credentials(), location=REGION)
        try:
            dataset = bq.get_dataset(dataset_id)
        except NotFound:
            dataset = None
        if dataset:
            if dataset.labels.get('purpose') != 'raw-replay-validation':
                raise ValueError('Refusing to delete a non-validation dataset')
            bq.delete_dataset(dataset, delete_contents=True)
    url = f'https://cloudfunctions.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/functions/{n["function"]}'
    meta = api(http, 'GET', url, allow_missing=True)
    if meta:
        if meta.get('labels', {}).get('purpose') != LABELS['purpose']:
            raise ValueError('Refusing to delete a non-validation function')
        operation(http, api(http, 'DELETE', url))
    assert api(http, 'GET', url, allow_missing=True) is None
    if state.get('transport'):
        assert state['transport'].startswith(f'projects/{PROJECT}/subscriptions/eventarc-{REGION}-{n["function"]}-')
        assert api(http, 'GET', 'https://pubsub.googleapis.com/v1/'+state['transport'], allow_missing=True) is None
    for kind, resources in [('subscriptions', n['subscriptions'].values()), ('topics', [n['trigger'], *n['topics'].values()])]:
        for name in resources:
            url = f'https://pubsub.googleapis.com/v1/projects/{PROJECT}/{kind}/{name}'
            meta = api(http, 'GET', url, allow_missing=True)
            if meta:
                if meta.get('labels', {}).get('purpose') != LABELS['purpose']:
                    raise ValueError('Refusing to delete a non-validation Pub/Sub resource')
                api(http, 'DELETE', url)
    for name in n['buckets'].values():
        bucket = gcs.bucket(name)
        if bucket.exists(timeout=10):
            bucket.reload(timeout=10)
            if bucket.labels.get('purpose') != LABELS['purpose']:
                raise ValueError('Refusing to delete a non-validation bucket')
            for blob in bucket.list_blobs(versions=True, timeout=10):
                if not blob.name.startswith(('acquisition-coverage/v1/', 'daily-inputs/v1/', 'probe-results/')):
                    raise ValueError('Unexpected object in validation bucket')
                blob.delete(if_generation_match=blob.generation, timeout=10)
            bucket.delete(timeout=30)
        assert not bucket.exists(timeout=10)
    for src in [*state.get('prior_sources', []), *([state['source']] if state.get('source') else [])]:
        blob = gcs.bucket(src['bucket']).blob(src['object'], generation=int(src['generation']))
        if blob.exists(timeout=10):
            blob.delete(if_generation_match=int(src['generation']), timeout=10)
    state['resources_deleted'] = True
    save(path, state)
    print(json.dumps({'resources_deleted': True, 'token': state['token']}))


def reconstruct(path):
    """Replay retained live inputs after their first deliveries were acknowledged.

    No source API calls or production routes are used. The unchanged actual
    consumer executes locally with an authenticated session against disposable
    BigQuery tables. Pub/Sub transport and sink writes are real.
    """
    from google.cloud import bigquery
    from google.api_core.exceptions import DeadlineExceeded
    from validate_ingestion import credentials, TABLES

    state = checked(path)
    assert state['phase'] == 'passed' and not state.get('resources_deleted')
    http, gcs, publisher, subscriber = clients()
    replay = runpy.run_path(str(ROOT/'scripts/operations/replay_daily_inputs.py'))
    n = state['names']
    bucket = gcs.bucket(n['buckets']['receipts'])
    archives = {}
    for blob in bucket.list_blobs(prefix='daily-inputs/v1/', timeout=30):
        if '/batches/' not in blob.name:
            continue
        payload = blob.download_as_bytes(if_generation_match=blob.generation, timeout=30)
        batch, rows = replay['verified_batch'](payload, blob.name.rsplit('/', 1)[1][:-5])
        archives[blob.name] = (batch, rows)
    assert len(archives) == 12, 'Expected live, initial, partial and corrected archive for each source'
    for feed, source in SOURCES.items():
        batches = [b for b, _ in archives.values() if b['source'] == source]
        assert len(batches) == 4
        outcomes = [json.loads(blob.download_as_bytes(if_generation_match=blob.generation, timeout=30))
                    for blob in bucket.list_blobs(prefix='daily-inputs/v1/'+source+'/', timeout=30) if '/outcomes/' in blob.name]
        partial = [o for o in outcomes if o['stage'] == 'publication_unconfirmed']
        assert len(outcomes) == 4 and len(partial) == 1 and partial[0]['acknowledged'] == 1
        assert len(archives[partial[0]['input']['object']][1]) == partial[0]['total'] > 1
        for case in ('live', 'fixture_initial', 'fixture_corrected'):
            receipt = state['cases'][feed][case]['receipt']
            ref = receipt['raw_input']
            assert ref['bucket'] == bucket.name
            blob = bucket.blob(ref['object'], generation=int(ref['generation']))
            assert hashlib.sha256(blob.download_as_bytes(if_generation_match=int(ref['generation']), timeout=30)).hexdigest() == ref['sha256']
            assert len(archives[ref['object']][1]) == receipt['published']
    dataset_id = PROJECT+'.raw_replay_validation_'+state['token']
    assert not state.get('replay_dataset'), 'Use retained evidence/cleanup after a partial reconstruction; do not duplicate writes'
    state['replay_dataset'] = dataset_id
    save(path, state)
    credential = credentials()
    bq = bigquery.Client(project=PROJECT, credentials=credential, location=REGION)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = REGION
    dataset.default_table_expiration_ms = 86_400_000
    dataset.labels = {'app': 'cocoa-prices', 'purpose': 'raw-replay-validation'}
    bq.create_dataset(dataset)
    package = runpy.run_path(str(ROOT/'scripts/deploy/package_functions.py'))['package_function']
    runtime, _ = package('ingestion', output_dir=path.parent/('replay-consumer-'+state['token']))
    consumer = runpy.run_path(str(runtime/'main.py'))
    scope = consumer['stage_analytics_data'].__globals__
    scope['session'] = lambda: http  # Existing operator credentials, actual insertAll request/write behavior.
    original_environment = {key: os.environ.get(key) for key in ('GCP_PROJECT', 'INGESTION_ROUTES')}
    routes = {f'projects/{PROJECT}/subscriptions/{n["subscriptions"][feed]}': {'feed': feed, 'table': dataset_id+'.'+TABLES[feed]} for feed in SOURCES}
    os.environ.update(GCP_PROJECT=PROJECT, INGESTION_ROUTES=json.dumps(routes))
    try:
        for feed, table in TABLES.items():
            bq.create_table(bigquery.Table(dataset_id+'.'+table, schema=bq.get_table(PROJECT+'.stream_staging.'+table).schema))
            ref = state['cases'][feed]['live']['receipt']['raw_input']
            batch, rows = archives[ref['object']]
            topic = f'projects/{PROJECT}/topics/{n["topics"][feed]}'
            subscription = f'projects/{PROJECT}/subscriptions/{n["subscriptions"][feed]}'
            replay['replay'](batch, publisher, topic, state['token']+':retained-live-inputs:'+feed)
            expected = {m['attributes']['observation_id'] for m in batch['messages']}
            delivered, deadline = set(), time.monotonic()+120
            while not expected <= delivered and time.monotonic() < deadline:
                try:
                    transport = subscriber.pull(request={'subscription': subscription, 'max_messages': 1000}, timeout=5, retry=None)
                except DeadlineExceeded:
                    continue
                for received in transport.received_messages:
                    attributes = dict(received.message.attributes)
                    # Previous duplicates can still arrive; only the labeled replay establishes this proof.
                    if attributes.get('replay_id') == hashlib.sha256((state['token']+':retained-live-inputs:'+feed).encode()).hexdigest():
                        envelope = {'subscription': subscription, 'message': {'data': base64.b64encode(received.message.data).decode(),
                                    'messageId': received.message.message_id, 'attributes': attributes}}
                        request = SimpleNamespace(method='POST', stream=io.BytesIO(json.dumps(envelope).encode()))
                        with contextlib.redirect_stdout(io.StringIO()):
                            result = consumer['stage_analytics_data'](request)
                        assert result[1] == 204, 'Actual consumer did not accept the retained input'
                        delivered.add(attributes['observation_id'])
                if transport.received_messages:
                    subscriber.acknowledge(request={'subscription': subscription, 'ack_ids': [r.ack_id for r in transport.received_messages]}, timeout=10, retry=None)
            assert expected <= delivered
            query = bq.query(f'SELECT * FROM `{dataset_id}.{table}`', job_config=bigquery.QueryJobConfig(maximum_bytes_billed=1073741824))
            persisted = {r['observation_id']: dict(r) for r in query.result(timeout=60)}
            assert set(persisted) == expected
            for message, row in zip(batch['messages'], rows, strict=True):
                saved = persisted[message['attributes']['observation_id']]
                assert saved['date'].isoformat() == row['date']
                assert saved['producer_acquired_at'].isoformat() == batch['acquired_at']
                assert saved['acquisition_id'] == batch['acquisition_id'] and saved['timestamp_status'] == 'valid'
                assert saved['raw_payload'] == row['raw_payload']
                assert all(saved[key] == row[key] for key in consumer['MEASUREMENTS'][feed])
            state.setdefault('raw_reconstruction', {})[feed] = {'archived_records': len(rows), 'replayed_revisions': len(delivered),
                'sink_revisions': len(persisted), 'provenance_and_values_match': True, 'query_job': query.job_id}
            save(path, state)
    finally:
        for key, value in original_environment.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    state['replay_without_upstream_or_retained_original_delivery'] = True
    save(path, state)
    print(json.dumps({'raw_archives_verified': len(archives), 'reconstruction': state['raw_reconstruction'], 'production_writes': False}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'refresh', 'exercise', 'reconstruct', 'cleanup'))
    parser.add_argument('--state', type=Path, required=True)
    args = parser.parse_args()
    globals()[args.action](args.state)
