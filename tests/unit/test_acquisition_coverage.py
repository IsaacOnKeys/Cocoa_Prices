"""Outage windows, revisions and generation races against a stateful storage fake."""

import json
import runpy
import sys
import time
from datetime import date
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

POLICY = runpy.run_path(str(Path(__file__).resolve().parents[2] / 'functions/shared/request_policy.py'))


class NotFound(Exception):
    pass


class PreconditionFailed(Exception):
    pass


class Store:
    def __init__(self):
        self.objects, self.generation, self.conflicts = {}, 0, 0
        self.fail_receipt = False

    def bucket(self, name):
        store = self

        class Bucket:
            def blob(self, path):
                key = (name, path)

                class Blob:
                    generation = None

                    def exists(self, **kwargs):
                        return key in store.objects

                    def reload(self, **kwargs):
                        if key not in store.objects:
                            raise NotFound()
                        self.generation = store.objects[key][0]

                    def download_as_bytes(self, **kwargs):
                        self.reload()
                        expected = kwargs.get('if_generation_match', self.generation)
                        if expected != self.generation:
                            raise PreconditionFailed()
                        return store.objects[key][1]

                    def upload_from_string(self, payload, *, if_generation_match, **kwargs):
                        assert kwargs['retry'] is None and 0 < kwargs['timeout'] <= 5
                        if store.fail_receipt and '/receipts/' in path:
                            raise RuntimeError('private-storage-canary')
                        current = store.objects.get(key, (0, None))[0]
                        if path.endswith('state.json') and store.conflicts:
                            store.conflicts -= 1
                            raise PreconditionFailed()
                        if current != if_generation_match:
                            raise PreconditionFailed()
                        store.generation += 1
                        store.objects[key] = (store.generation, payload.encode() if isinstance(payload, str) else payload)

                return Blob()

        return Bucket()


@pytest.fixture
def cloud(monkeypatch):
    exceptions = ModuleType('google.api_core.exceptions')
    exceptions.NotFound, exceptions.PreconditionFailed = NotFound, PreconditionFailed
    monkeypatch.setitem(sys.modules, exceptions.__name__, exceptions)
    store = Store()

    def begin(today='2026-10-29'):
        return POLICY['Coverage'](store, 'EIA', date.fromisoformat(today), 'receipts', 'states', deadline=time.monotonic()+50)

    def receipt(today='2026-10-29', price=72.5, acquired=None):
        rows = [{'date': today, 'oil_price': price}]
        record = {'source': 'EIA', 'status': 'published', 'published': 1, 'observation_start': today,
                  'observation_end': today, 'acquired_at': acquired or today+'T12:00:00+00:00', 'acquisition_id': 'a'*64}
        return record, rows

    return SimpleNamespace(store=store, begin=begin, receipt=receipt)


def state(cloud):
    return json.loads(cloud.store.objects[('states', 'acquisition-coverage/v1/EIA/state.json')][1])


def test_outage_expands_window_and_revised_old_observation_is_retained(cloud):
    first = cloud.begin('2026-10-20')
    first.complete(*cloud.receipt('2026-10-10', price=70, acquired='2026-10-20T12:00:00+00:00'))
    recovered = cloud.begin()
    assert recovered.start == date(2026, 9, 20) and recovered.end == date(2026, 10, 29)
    recovered.complete(*cloud.receipt('2026-10-10', price=71, acquired='2026-10-29T12:00:00+00:00'))
    assert state(cloud)['window_end'] == '2026-10-29'
    receipts = [json.loads(v[1]) for k, v in cloud.store.objects.items() if '/receipts/' in k[1]]
    assert len(receipts) == 2
    assert receipts[0]['observations'][0]['observation_id'] != receipts[1]['observations'][0]['observation_id']
    assert '2026-10-25' in receipts[1]['not_exposed_dates']
    assert 'market closures' in receipts[1]['absence_basis']


def test_out_of_order_completion_does_not_regress_frontier_or_latest_receipt(cloud):
    older, newer = cloud.begin('2026-10-20'), cloud.begin()
    newer.complete(*cloud.receipt())
    before = state(cloud)
    older.complete(*cloud.receipt('2026-10-20'))
    after = state(cloud)
    assert after['window_end'] == before['window_end'] == '2026-10-29'
    assert after['acquired_at'] == before['acquired_at']
    assert after['receipt_object'] == before['receipt_object']
    assert after['window_start'] == '2026-09-20'


def test_state_cas_retries_are_finite_and_receipts_survive_failure(cloud):
    progress = cloud.begin()
    cloud.store.conflicts = 2
    progress.complete(*cloud.receipt())
    assert state(cloud)['window_end'] == '2026-10-29'
    before = state(cloud)
    cloud.store.conflicts = 3
    with pytest.raises(RuntimeError, match='persistence failed'):
        cloud.begin('2026-10-30').complete(*cloud.receipt('2026-10-30'))
    assert state(cloud) == before
    assert len([k for k in cloud.store.objects if '/receipts/' in k[1]]) == 2


def test_failed_receipt_write_never_advances_coverage_or_leaks_sdk_error(cloud):
    progress = cloud.begin()
    cloud.store.fail_receipt = True
    with pytest.raises(RuntimeError, match='persistence failed') as error:
        progress.complete(*cloud.receipt())
    assert not cloud.store.objects and 'private-storage-canary' not in str(error.value)


def test_repeated_completion_is_idempotent_and_explicit_missing_is_distinct(cloud):
    progress = cloud.begin()
    record, rows = cloud.receipt(price=None)
    progress.complete(record, rows)
    progress.complete(record, rows)
    receipts = [json.loads(v[1]) for k, v in cloud.store.objects.items() if '/receipts/' in k[1]]
    assert len(receipts) == 1 and receipts[0]['observations'][0]['missing_measurements'] == ['oil_price']
    assert '2026-10-29' not in receipts[0]['not_exposed_dates']


def test_empty_source_inventory_is_explicit_and_not_a_collection_failure(cloud):
    record, _ = cloud.receipt()
    record.update(status='no_observations', published=0, observation_start=None, observation_end=None)
    progress = cloud.begin()
    progress.complete(record, [])
    receipt = json.loads(next(v[1] for k, v in cloud.store.objects.items() if '/receipts/' in k[1]))
    assert receipt['observations'] == [] and len(receipt['not_exposed_dates']) == 31


@pytest.mark.parametrize('field,value', [('acquired_at', '2026-10-29T12:00:00'), ('published', 2),
                                      ('acquisition_id', '../escape'), ('observation_end', '2026-10-30'),
                                      ('source', 'ICCO')])
def test_invalid_receipt_is_rejected_before_any_storage_write(cloud, field, value):
    progress = cloud.begin()
    record, rows = cloud.receipt()
    record[field] = value
    with pytest.raises(RuntimeError, match='persistence failed'):
        progress.complete(record, rows)
    assert not cloud.store.objects


def test_outage_beyond_bound_requires_backfill_without_advancing(cloud):
    cloud.begin('2026-07-01').complete(*cloud.receipt('2026-07-01'))
    before = state(cloud)
    with pytest.raises(POLICY['RecoveryRequired'], match='90-day bound'):
        cloud.begin()
    assert state(cloud) == before


def test_lost_projection_cannot_silently_reset_baseline(cloud):
    cloud.begin().complete(*cloud.receipt())
    del cloud.store.objects[('states', 'acquisition-coverage/v1/EIA/state.json')]
    with pytest.raises(POLICY['RecoveryRequired'], match='rebuild'):
        cloud.begin()


def test_partial_configuration_fails_instead_of_silently_disabling_coverage(monkeypatch):
    monkeypatch.setenv('COVERAGE_RECEIPT_BUCKET', 'receipts')
    monkeypatch.delenv('COVERAGE_STATE_BUCKET', raising=False)
    with pytest.raises(RuntimeError, match='Incomplete'):
        POLICY['coverage']('EIA', date(2026, 10, 29), deadline=time.monotonic()+50)
