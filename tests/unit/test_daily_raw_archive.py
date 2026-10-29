"""Durable inputs survive partial/unknown transport and replay without upstream IO."""

import copy
import hashlib
import json
import runpy
import time
from pathlib import Path
from unittest.mock import Mock

import pytest

from test_acquisition_coverage import cloud as storage_fixture

ROOT = Path(__file__).resolve().parents[2]
POLICY = runpy.run_path(str(ROOT / 'functions/shared/request_policy.py'))
REPLAY = runpy.run_path(str(ROOT / 'scripts/operations/replay_daily_inputs.py'))
SCHEMA = json.loads((ROOT / 'schemas/oil_schema.avsc').read_bytes())
ACQUIRED = '2026-10-29T12:00:00+00:00'


@pytest.fixture
def archive(monkeypatch):
    cloud = storage_fixture.__wrapped__(monkeypatch)
    publisher = Mock()
    publisher.topic_path.return_value = 'projects/unit-project/topics/isolated-recovery'
    records = [{'date': d, 'oil_price': p, 'ingestion_time': ACQUIRED,
                'raw_payload': json.dumps({'source': 'EIA', 'series': 'RBRTE', 'published_verified': True})}
               for d, p in [('2026-10-28', 72.5), ('2026-10-29', None)]]

    def publish(factory=None, rows=None):
        return POLICY['publish_batch'](factory or (lambda: publisher), 'unit-project', 'isolated-recovery',
                                       records if rows is None else rows, SCHEMA, source='EIA', acquisition_id='a'*64,
                                       acquired_at=ACQUIRED, deadline=time.monotonic()+50, progress=cloud.begin())

    return cloud, publisher, records, publish


def documents(cloud, kind):
    return [(key, value[0], value[1]) for key, value in cloud.store.objects.items() if '/'+kind+'/' in key[1]]


def test_entire_batch_is_durable_before_publisher_initialization(archive):
    cloud, publisher, records, publish = archive

    def initialize():
        batches = documents(cloud, 'batches')
        assert len(batches) == 1 and len(json.loads(batches[0][2])['messages']) == len(records)
        return publisher

    receipt = publish(initialize)
    key, generation, data = documents(cloud, 'batches')[0]
    batch, decoded = REPLAY['verified_batch'](data, receipt['raw_input']['sha256'])
    assert receipt['raw_input']['generation'] == str(generation)
    assert receipt['raw_input']['object'] == key[1]
    assert decoded == records and batch['schema'] == SCHEMA
    assert batch['request']['series'] == 'RBRTE' and 'api_key' not in data.decode()
    outcome = json.loads(documents(cloud, 'outcomes')[0][2])
    assert outcome['stage'] == 'publish_acknowledged' and outcome['acknowledged'] == 2
    publisher.publish.assert_called()


@pytest.mark.parametrize('failure', ['initialize', 'second_publish', 'outcome'])
def test_unknown_or_partial_transport_keeps_complete_reconstructable_input(archive, monkeypatch, failure):
    cloud, publisher, records, publish = archive
    factory = None
    if failure == 'initialize':
        factory = Mock(side_effect=RuntimeError('private-sdk-canary'))
    elif failure == 'second_publish':
        publisher.publish.return_value.result.side_effect = [None, RuntimeError('private-sdk-canary')]
    else:
        original = POLICY['_archive_json']

        def write(progress, prefix, document):
            if '/outcomes/' in prefix:
                raise RuntimeError('private-sdk-canary')
            return original(progress, prefix, document)

        monkeypatch.setitem(POLICY['publish_batch'].__globals__, '_archive_json', write)
    with pytest.raises(RuntimeError) as error:
        publish(factory)
    assert 'private-sdk-canary' not in str(error.value)
    data = documents(cloud, 'batches')[0][2]
    _, decoded = REPLAY['verified_batch'](data, hashlib.sha256(data).hexdigest())
    assert decoded == records
    outcomes = documents(cloud, 'outcomes')
    if failure == 'outcome':
        assert not outcomes
    else:
        outcome = json.loads(outcomes[0][2])
        assert outcome['stage'] == 'publication_unconfirmed'
        assert outcome['acknowledged'] == (1 if failure == 'second_publish' else 0)


def test_failed_input_archive_prevents_even_publisher_initialization(archive, monkeypatch):
    _, publisher, _, publish = archive
    monkeypatch.setitem(POLICY['publish_batch'].__globals__, '_archive_json', Mock(side_effect=RuntimeError('private-storage-canary')))
    factory = Mock(return_value=publisher)
    with pytest.raises(RuntimeError, match='publication withheld'):
        publish(factory)
    factory.assert_not_called()
    publisher.publish.assert_not_called()


def test_empty_successful_window_is_archived_with_explicit_zero_acknowledgements(archive):
    cloud, publisher, _, publish = archive
    receipt = publish(rows=[])
    data = documents(cloud, 'batches')[0][2]
    assert REPLAY['verified_batch'](data, receipt['raw_input']['sha256'])[1] == []
    assert json.loads(documents(cloud, 'outcomes')[0][2])['acknowledged'] == 0
    publisher.publish.assert_not_called()


@pytest.mark.parametrize('tamper', ['outer_checksum', 'schema', 'message', 'identity', 'timestamp', 'request', 'trailing_bytes'])
def test_corrupted_or_inconsistent_archive_is_rejected_before_replay(archive, tamper):
    cloud, _, _, publish = archive
    publish()
    original = documents(cloud, 'batches')[0][2]
    batch = copy.deepcopy(json.loads(original))
    if tamper == 'schema':
        batch['schema']['name'] = 'Changed'
    elif tamper == 'message':
        batch['messages'][0]['sha256'] = '0'*64
    elif tamper == 'identity':
        batch['messages'][0]['attributes']['acquisition_id'] = 'b'*64
    elif tamper == 'timestamp':
        batch['acquired_at'] = batch['fetched_at'] = '2026-10-29T12:01:00+00:00'
    elif tamper == 'request':
        batch['request']['api_key'] = 'must-not-be-archived'
    elif tamper == 'trailing_bytes':
        import base64

        wire = base64.b64decode(batch['messages'][0]['data']) + b'garbage'
        batch['messages'][0].update(data=base64.b64encode(wire).decode(), sha256=hashlib.sha256(wire).hexdigest())
    data = POLICY['_json_bytes'](batch)
    with pytest.raises(ValueError):
        REPLAY['verified_batch'](data, '0'*64 if tamper == 'outer_checksum' else hashlib.sha256(data).hexdigest())


def test_replay_uses_only_archived_bytes_and_preserves_old_acquisition(archive):
    cloud, _, records, publish = archive
    receipt = publish()
    data = documents(cloud, 'batches')[0][2]
    batch, decoded = REPLAY['verified_batch'](data, receipt['raw_input']['sha256'])
    recovered = Mock()
    assert REPLAY['replay'](batch, recovered, 'projects/unit-project/topics/recovery-topic', 'incident-123') == 2
    assert decoded == records
    for call, message in zip(recovered.publish.call_args_list, batch['messages'], strict=True):
        assert call.kwargs['acquisition_id'] == 'a'*64
        assert call.kwargs['observation_id'] == message['attributes']['observation_id']
        assert call.kwargs['replay_id'] == hashlib.sha256(b'incident-123').hexdigest()
        assert 'incident-123' not in str(call)


def test_duplicate_create_confirms_existing_bytes_without_overwrite(archive):
    cloud, _, _, publish = archive
    first = publish()['raw_input']
    second = publish()['raw_input']
    assert first == second and len(documents(cloud, 'batches')) == 1
