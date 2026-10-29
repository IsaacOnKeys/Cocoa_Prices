"""Guarded live-source/outage/revision validation in disposable managed resources."""

import argparse
import hashlib
import io
import json
import re
import runpy
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
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
                if not blob.name.startswith(('acquisition-coverage/v1/', 'probe-results/')):
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'refresh', 'exercise', 'cleanup'))
    parser.add_argument('--state', type=Path, required=True)
    args = parser.parse_args()
    globals()[args.action](args.state)
