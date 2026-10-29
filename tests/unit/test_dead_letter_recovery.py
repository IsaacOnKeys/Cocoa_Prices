"""Recovery preserves payload/provenance and bounds poison and uncertain replay attempts."""

import io
import json
import runpy
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import fastavro
import pytest

M = runpy.run_path(str(Path(__file__).resolve().parents[2] / 'scripts/operations/replay_dead_letters.py'))
PROJECT = 'unit-project'
TOPIC = f'projects/{PROJECT}/topics/input'
DEAD = f'projects/{PROJECT}/topics/input-dead-letter'
SCHEMA = {'type': 'record', 'name': 'PubsubMessageWithMetadata', 'fields': [
    {'name': 'subscription_name', 'type': 'string'}, {'name': 'message_id', 'type': 'string'},
    {'name': 'publish_time', 'type': {'type': 'long', 'logicalType': 'timestamp-micros'}},
    {'name': 'attributes', 'type': {'type': 'map', 'values': 'string'}}, {'name': 'data', 'type': 'bytes'}]}


@pytest.fixture
def recovery(monkeypatch):
    class NotFound(Exception):
        pass

    class PreconditionFailed(Exception):
        pass

    exceptions = ModuleType('google.api_core.exceptions')
    exceptions.NotFound, exceptions.PreconditionFailed = NotFound, PreconditionFailed
    monkeypatch.setitem(sys.modules, exceptions.__name__, exceptions)

    objects = {}

    class Blob:
        def __init__(self, name):
            self.name = name

        def reload(self, **kwargs):
            if self.name not in objects:
                raise NotFound('absent')
            self.generation, data = objects[self.name]
            self.size = len(data)

        def download_as_bytes(self, **kwargs):
            return objects[self.name][1]

        def upload_from_string(self, data, *, if_generation_match, **kwargs):
            current = objects.get(self.name, (0, None))[0]
            if current != if_generation_match:
                raise PreconditionFailed('concurrent reservation')
            self.generation = current + 1
            objects[self.name] = self.generation, data

    bucket = SimpleNamespace(blob=Blob)
    publisher, subscriber = Mock(), Mock()
    subscriber.get_subscription.return_value = SimpleNamespace(topic=TOPIC, dead_letter_policy=SimpleNamespace(dead_letter_topic=DEAD))
    publisher.publish.return_value.result.return_value = 'replayed-message'
    record = {'subscription_name': f'projects/{PROJECT}/subscriptions/input-dead-letter-sub', 'message_id': 'dead-message',
              'publish_time': datetime.now(timezone.utc), 'data': b'\x00original-avro\xff', 'attributes': {
                  'CloudPubSubDeadLetterSourceSubscription': 'input-sub',
                  'CloudPubSubDeadLetterSourceSubscriptionProject': PROJECT,
                  'CloudPubSubDeadLetterSourceTopicPublishTime': '2026-10-29T10:00:00+00:00',
                  'CloudPubSubDeadLetterSourceDeliveryCount': '5', 'acquisition_id': 'a'*64, 'observation_id': 'b'*64}}
    kwargs = dict(bucket=bucket, publisher=publisher, subscriber=subscriber, project=PROJECT,
                  dead_letter=DEAD, topic=TOPIC, key='repair-1', evidence={'uri': 'pinned-original'})
    return record, kwargs, objects


def test_export_checksum_and_complete_container(recovery):
    record, _, _ = recovery
    stream = io.BytesIO()
    fastavro.writer(stream, SCHEMA, [record, record])
    data = stream.getvalue()
    assert M['verified_records'](data, M['digest'](data)) == [record, record]
    for bad in [data[:-1], b'not-avro']:
        with pytest.raises((ValueError, EOFError)):
            M['verified_records'](bad, M['digest'](bad))
    with pytest.raises(ValueError, match='checksum'):
        M['verified_records'](data, '0'*64)


def test_replay_original_bytes_with_durable_poison_budget(recovery):
    record, kw, objects = recovery
    result = M['replay'](record, **kw)
    args, attrs = kw['publisher'].publish.call_args
    assert args == (TOPIC, record['data'])
    assert attrs['acquisition_id'] == 'a'*64 and attrs['observation_id'] == 'b'*64
    assert not any(k.startswith('CloudPubSub') for k in attrs)
    assert attrs['recovery_root'] == result['recovery_root'] and attrs['recovery_attempt'] == '1'
    assert json.loads(next(iter(objects.values()))[1])['attempts'][0]['status'] == 'confirmed'
    with pytest.raises(ValueError, match='budget'):
        M['replay'](record, **{**kw, 'key': 'repair-2'})
    # A poison message forwarded again retains its root, despite a new transport timestamp.
    record['attributes'].update({k: v for k, v in attrs.items() if k not in ('retry', 'timeout')})
    record['attributes']['CloudPubSubDeadLetterSourceTopicPublishTime'] = '2026-10-29T11:00:00+00:00'
    for attempt in (2, 3):
        assert M['replay'](record, **{**kw, 'key': f'repair-{attempt}', 'max_replays': attempt})['attempt'] == attempt
    with pytest.raises(ValueError, match='budget'):
        M['replay'](record, **{**kw, 'key': 'repair-4', 'max_replays': 3})


def test_uncertain_send_and_duplicate_key_require_reconciliation(recovery):
    record, kw, objects = recovery
    kw['publisher'].publish.return_value.result.side_effect = TimeoutError('unknown acknowledgement')
    with pytest.raises(TimeoutError):
        M['replay'](record, **kw)
    assert json.loads(next(iter(objects.values()))[1])['attempts'][0]['status'] == 'pending'
    for changed in [dict(key='repair-2', max_replays=2), dict(max_replays=2, allow_uncertain=True)]:
        with pytest.raises(ValueError, match='budget'):
            M['replay'](record, **{**kw, **changed})
    assert kw['publisher'].publish.call_count == 1
    kw['publisher'].publish.return_value.result.side_effect = None
    assert M['replay'](record, **{**kw, 'key': 'reconciled-2', 'max_replays': 2, 'allow_uncertain': True})['attempt'] == 2


@pytest.mark.parametrize('change', ['topic', 'dead_letter', 'project', 'identity', 'ordered'])
def test_route_or_identity_changes_never_publish(recovery, change):
    record, kw, _ = recovery
    if change in ('topic', 'dead_letter', 'project'):
        kw[change] = 'different'
    elif change == 'identity':
        record['attributes']['recovery_root'] = '0'*64
    else:
        record['attributes']['ordering_key'] = 'ordered'
    with pytest.raises(ValueError):
        M['replay'](record, **kw)
    kw['publisher'].publish.assert_not_called()


def test_cas_conflict_never_publishes(recovery):
    from google.api_core.exceptions import PreconditionFailed

    record, kw, _ = recovery
    blob = Mock()
    blob.size = 2
    blob.generation = 1
    root, fingerprint, _ = M['identity'](record, f'projects/{PROJECT}/subscriptions/input-sub', TOPIC)
    blob.download_as_bytes.return_value = json.dumps({'version': 1, 'identity': fingerprint, 'attempts': []}).encode()
    blob.upload_from_string.side_effect = PreconditionFailed('concurrent operator')
    kw['bucket'] = SimpleNamespace(blob=lambda name: blob)
    with pytest.raises(PreconditionFailed):
        M['replay'](record, **kw)
    kw['publisher'].publish.assert_not_called()
    assert len(root) == 64
