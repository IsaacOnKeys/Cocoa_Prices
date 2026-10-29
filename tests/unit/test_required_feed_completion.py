"""Run-bound receipts, source eligibility and complete logical staging inventory."""

import ast
import copy
import hashlib
import json
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pendulum
import pytest

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = {'cocoa': {'source': 'ICCO', 'measurements': ('cocoa_price',), 'maximum_age': 7},
            'oil': {'source': 'EIA', 'measurements': ('oil_price',), 'maximum_age': 14},
            'weather': {'source': 'NASA_POWER', 'measurements': ('precipitation', 'soil_moisture'), 'maximum_age': 7}}


@pytest.fixture
def gate(monkeypatch):
    class AirflowException(Exception):
        pass

    class AirflowFailException(AirflowException):
        pass

    class ClockMeta(type):
        def __instancecheck__(cls, value):
            return isinstance(value, datetime)

    class Clock(datetime, metaclass=ClockMeta):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 29, 18, 0, 20, tzinfo=timezone.utc)

    source = ast.parse((ROOT/'airflow/dags/cocoa_daily.py').read_bytes())
    names = {'_utc', '_run_scope', 'capture_baseline_fn', '_expected_acquisition', '_check_timeout',
             '_receipt_document', '_latest_completion', '_source_ages', '_staged_inventory', 'make_wait_callable'}
    nodes = [node for node in source.body if isinstance(node, ast.FunctionDef) and node.name in names]
    storage, bq = Mock(), Mock()
    storage.list_blobs.return_value = []
    bq.query.return_value.result.return_value = []
    sdk = ModuleType('google.cloud.bigquery')
    sdk.QueryJobConfig = lambda **kwargs: SimpleNamespace(**kwargs)
    sdk.ScalarQueryParameter = lambda name, kind, value: SimpleNamespace(name=name, kind=kind, value=value)
    cloud = ModuleType('google.cloud')
    cloud.bigquery = sdk
    monkeypatch.setitem(sys.modules, cloud.__name__, cloud)
    monkeypatch.setitem(sys.modules, sdk.__name__, sdk)
    scope = {'datetime': Clock, 'date': date, 'timedelta': timedelta, 'timezone': timezone, 'pendulum': pendulum,
             'TZ': pendulum.timezone('Europe/Berlin'), 'hashlib': hashlib, 'json': json, 're': re, 'time': time,
             'SOURCE_CONTRACT': copy.deepcopy(CONTRACT), 'GCP_PROJECT': 'cocoa-prices-430315', 'BQ_LOC': 'europe-west3',
             'COVERAGE_BUCKET': 'isolated-coverage', 'GCSHook': Mock(return_value=SimpleNamespace(get_conn=lambda: storage)),
             'BigQueryHook': Mock(return_value=SimpleNamespace(get_client=lambda **kw: bq)),
             'AirflowException': AirflowException, 'AirflowFailException': AirflowFailException,
             'FEEDS': [{'key': key, 'trigger_topic': topic} for key, topic in
                       [('cocoa', None), ('oil', 'eia-oil-trigger'), ('weather', 'weather-trigger')]]}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'required_feed_completion', 'exec'), scope)
    run = SimpleNamespace(dag_id='cocoa_daily', run_id='unit-required', run_type='scheduled', conf={},
                          start_date=datetime(2026, 10, 29, 18, tzinfo=timezone.utc),
                          data_interval_end=datetime(2026, 10, 29, 18, tzinfo=timezone.utc))
    run_scope = scope['_run_scope'](run)
    ti = Mock()
    ti.xcom_pull.side_effect = lambda task_ids, key=None: run_scope if task_ids == 'capture_baseline' else {
        'invoked': True, 'invocation_started_at': '2026-10-29T18:00:01+00:00'}
    return SimpleNamespace(ns=scope, storage=storage, bq=bq, run=run, run_scope=run_scope, ti=ti,
                           permanent=AirflowFailException, transient=AirflowException)


def receipt(gate, feed='weather', *, missing_days=0):
    today, start = date(2026, 10, 29), date(2026, 9, 29)
    observed = [start+timedelta(days=i) for i in range(31)] if feed == 'weather' else [today-timedelta(days=2), today-timedelta(days=1)]
    measurements = CONTRACT[feed]['measurements']
    inventory = []
    for value in observed:
        missing = list(measurements) if (today-value).days < missing_days else []
        inventory.append({'date': value.isoformat(), 'observation_id': hashlib.sha256((feed+value.isoformat()+str(missing)).encode()).hexdigest(),
                          'missing_measurements': missing})
    return {'version': 1, 'source': CONTRACT[feed]['source'], 'stage': 'publish_acknowledged', 'status': 'published',
            'published': len(inventory), 'acquisition_id': gate.ns['_expected_acquisition'](feed, gate.run),
            'acquired_at': '2026-10-29T18:00:03+00:00', 'window_start': start.isoformat(), 'window_end': today.isoformat(),
            'observation_start': inventory[0]['date'], 'observation_end': inventory[-1]['date'],
            'observations': inventory, 'not_exposed_dates': sorted({(start+timedelta(days=i)).isoformat() for i in range(31)}-{r['date'] for r in inventory})}


def install(gate, document, feed='weather'):
    data = json.dumps(document, sort_keys=True, separators=(',', ':')).encode()
    digest = hashlib.sha256(data).hexdigest()
    prefix = f"acquisition-coverage/v1/{CONTRACT[feed]['source']}/receipts/{document['acquisition_id']}/"
    blob = SimpleNamespace(name=prefix+digest+'.json', generation=123, size=len(data), download_as_bytes=Mock(return_value=data))
    gate.storage.list_blobs.return_value = [blob]
    rows = []
    for item in document['observations']:
        row = {'date': date.fromisoformat(item['date']), 'observation_id': item['observation_id'], 'provenance_errors': 0}
        for measurement in CONTRACT[feed]['measurements']:
            row[measurement+'_missing_any'] = row[measurement+'_missing_all'] = measurement in item['missing_measurements']
        rows.append(row)
    gate.bq.query.return_value.result.return_value = rows
    return rows, blob


def poke(gate, feed='weather'):
    return gate.ns['make_wait_callable'](feed, 'isolated_project.validation.staging')(ti=gate.ti, dag_run=gate.run)


@pytest.mark.parametrize('feed', ['cocoa', 'oil', 'weather'])
def test_complete_current_source_and_sink_inventory_passes(gate, feed):
    document = receipt(gate, feed)
    _, blob = install(gate, document, feed)
    assert poke(gate, feed) is True
    proof = gate.ti.xcom_push.call_args.kwargs
    assert proof['key'] == 'accepted_source' and proof['value']['observations'] == document['published']
    assert proof['value']['acquisition_id'] == document['acquisition_id']
    assert blob.download_as_bytes.call_args.kwargs['if_generation_match'] == 123
    assert gate.storage.list_blobs.call_args.kwargs['retry'] is None
    query = gate.bq.query.call_args
    assert '@acquisition_id' in query.args[0] and document['acquisition_id'] not in query.args[0]
    assert 'GROUP BY date,observation_id' in query.args[0]
    assert query.kwargs['job_config'].maximum_bytes_billed == 1_073_741_824


def test_late_last_row_holds_publication_until_the_entire_inventory_arrives(gate):
    rows, _ = install(gate, receipt(gate))
    final = rows.pop()
    assert poke(gate) is False
    gate.ti.xcom_push.assert_not_called()
    rows.append(final)
    assert poke(gate) is True


def test_duplicate_deliveries_do_not_inflate_or_invalidate_logical_coverage(gate):
    rows, _ = install(gate, receipt(gate))
    rows.extend(copy.deepcopy(rows))
    assert poke(gate) is True


def test_unrelated_new_staging_rows_cannot_replace_a_current_completion_receipt(gate):
    gate.bq.query.return_value.result.return_value = [{'date': date(2026, 10, 29), 'observation_id': 'a'*64}]*100
    assert poke(gate) is False
    gate.bq.query.assert_not_called()


def test_prior_trigger_attempt_receipt_is_not_current_progress(gate):
    document = receipt(gate)
    document['acquired_at'] = '2026-10-29T17:59:00+00:00'
    install(gate, document)
    assert poke(gate) is False
    gate.bq.query.assert_not_called()


def test_actual_eight_day_nasa_gap_fails_before_transform_or_sink_query(gate):
    install(gate, receipt(gate, missing_days=8))
    with pytest.raises(gate.permanent, match='source age 8 exceeds 7'):
        poke(gate)
    gate.bq.query.assert_not_called()
    gate.ti.xcom_push.assert_not_called()


def test_each_weather_measurement_must_independently_meet_the_age_limit(gate):
    document = receipt(gate, missing_days=8)
    for row in document['observations']:
        row['missing_measurements'] = [m for m in row['missing_measurements'] if m == 'soil_moisture']
    install(gate, document)
    with pytest.raises(gate.permanent, match='soil_moisture source age 8'):
        poke(gate)


def test_explicit_missing_today_is_allowed_when_latest_valid_measurement_is_young(gate):
    install(gate, receipt(gate, missing_days=2))
    assert poke(gate)
    ages = gate.ti.xcom_push.call_args.kwargs['value']['source_ages']
    assert ages['precipitation']['age_days'] == ages['soil_moisture']['age_days'] == 2


def test_newer_missing_source_cannot_fall_back_to_an_older_eligible_completion(gate):
    install(gate, receipt(gate))
    older = gate.storage.list_blobs.return_value[0]
    latest = receipt(gate, missing_days=8)
    latest['acquired_at'] = '2026-10-29T18:00:04+00:00'
    install(gate, latest)
    gate.storage.list_blobs.return_value.append(older)
    with pytest.raises(gate.permanent, match='source age 8'):
        poke(gate)


@pytest.mark.parametrize('feed,age,accepted', [('cocoa', 7, True), ('cocoa', 8, False),
                                            ('oil', 14, True), ('oil', 15, False)])
def test_source_age_boundaries_are_inclusive_and_feed_specific(gate, feed, age, accepted):
    document = receipt(gate, feed)
    document['observations'] = [{'date': (date(2026, 10, 29)-timedelta(days=age)).isoformat(), 'missing_measurements': []}]
    if accepted:
        assert next(iter(gate.ns['_source_ages'](feed, document, gate.run_scope).values()))['age_days'] == age
    else:
        with pytest.raises(gate.permanent, match='source age'):
            gate.ns['_source_ages'](feed, document, gate.run_scope)


@pytest.mark.parametrize('mutation', ['missing_date', 'duplicate_date', 'wrong_source', 'wrong_identity', 'naive_time',
                                      'wrong_window', 'wrong_count', 'future_time', 'wrong_missing_field'])
def test_invalid_or_incomplete_completion_receipt_cannot_establish_coverage(gate, mutation):
    document = receipt(gate)
    if mutation == 'missing_date':
        document['observations'].pop(0)
        document['published'] -= 1
        document['observation_start'] = document['observations'][0]['date']
    elif mutation == 'duplicate_date':
        document['observations'][1]['date'] = document['observations'][0]['date']
    elif mutation == 'wrong_source':
        document['source'] = 'ICCO'
    elif mutation == 'wrong_identity':
        document['acquisition_id'] = 'b'*64
    elif mutation == 'naive_time':
        document['acquired_at'] = '2026-10-29T18:00:03'
    elif mutation == 'wrong_window':
        document['window_start'] = '2026-10-28'
    elif mutation == 'wrong_count':
        document['published'] = True
    elif mutation == 'future_time':
        document['acquired_at'] = '2026-10-29T18:06:00+00:00'
    else:
        document['observations'][0]['missing_measurements'] = ['other_product']
    with pytest.raises(gate.permanent, match='invalid acquisition'):
        gate.ns['_receipt_document'](json.dumps(document), gate.ns['_expected_acquisition']('weather', gate.run),
                                     'weather', gate.run_scope, '2026-10-29T18:00:01+00:00')


@pytest.mark.parametrize('failure', ['checksum', 'oversize', 'too_many', 'timestamp_tie'])
def test_untrusted_or_ambiguous_completion_objects_fail_closed(gate, failure):
    _, blob = install(gate, receipt(gate))
    if failure == 'checksum':
        blob.download_as_bytes.return_value += b' '
    elif failure == 'oversize':
        blob.size = 262_145
    elif failure == 'too_many':
        gate.storage.list_blobs.return_value = [blob]*33
    else:
        gate.storage.list_blobs.return_value = [blob, blob]
    with pytest.raises(gate.permanent):
        poke(gate)


@pytest.mark.parametrize('failure', ['unlisted_revision', 'invalid_timestamp', 'missingness_mismatch'])
def test_sink_contradictions_block_the_release(gate, failure):
    rows, _ = install(gate, receipt(gate))
    if failure == 'unlisted_revision':
        rows[0]['observation_id'] = 'c'*64
    elif failure == 'invalid_timestamp':
        rows[0]['provenance_errors'] = 1
    else:
        rows[0]['precipitation_missing_any'] = True
    with pytest.raises(gate.permanent, match='contradicts'):
        poke(gate)


@pytest.mark.parametrize('boundary', ['storage', 'bigquery'])
def test_transient_io_is_repolled_within_the_overall_sensor_timeout(gate, caplog, boundary):
    install(gate, receipt(gate))
    if boundary == 'storage':
        gate.storage.list_blobs.side_effect = RuntimeError('private-sdk-canary')
    else:
        gate.bq.query.side_effect = RuntimeError('private-sdk-canary')
    assert poke(gate) is False
    gate.ti.xcom_push.assert_not_called()
    assert 'private-sdk-canary' not in caplog.text


def test_scheduled_business_date_uses_interval_end_and_supports_enum_run_types(gate):
    class Kind(str, Enum):
        SCHEDULED = 'scheduled'
    gate.run.run_type = Kind.SCHEDULED
    assert gate.ns['_run_scope'](gate.run)['business_date'] == '2026-10-29'


@pytest.mark.parametrize('stamp,expected', [('2026-03-30T17:00:00+00:00', '2026-03-30T16:30:00+00:00'),
                                         ('2026-10-26T18:00:00+00:00', '2026-10-26T17:30:00+00:00')])
def test_berlin_service_window_tracks_daylight_saving(gate, stamp, expected):
    gate.run.start_date = gate.run.data_interval_end = datetime.fromisoformat(stamp)
    result = gate.ns['_run_scope'](gate.run)
    assert result['not_before'] == stamp
    gate.run.start_date = datetime.fromisoformat(expected)-timedelta(seconds=1)
    with pytest.raises(gate.permanent, match='outside'):
        gate.ns['_run_scope'](gate.run)


def test_manual_engineering_flag_does_not_relax_source_freshness(gate):
    gate.run.run_type = 'manual'
    gate.run.start_date = datetime(2026, 10, 29, 9, tzinfo=timezone.utc)
    with pytest.raises(gate.permanent, match='outside'):
        gate.ns['_run_scope'](gate.run)
    gate.run.conf = {'engineering_validation': True}
    scope = gate.ns['_run_scope'](gate.run)
    with pytest.raises(gate.permanent, match='source age 8'):
        gate.ns['_source_ages']('weather', receipt(gate, missing_days=8), scope)


def test_string_validation_flag_cannot_bypass_the_service_window(gate):
    gate.run.conf = {'engineering_validation': 'false'}
    with pytest.raises(gate.permanent, match='explicit boolean'):
        gate.ns['_run_scope'](gate.run)


@pytest.mark.parametrize('feed,expected', [
    ('cocoa', '34c1c5b08db9be82fdccd50219576c396705b7f34c91fe1aeb6db7bbc320ac31'),
    ('oil', '752bafc42bae1ddc486e6b2f8331875b4fcc9ee84c886041e2ee180340265522'),
    ('weather', 'defff401bc7ddaf80bccf685dad76fced175f1163940ddc61ff3f6057ccf969b'),
])
def test_identity_v1_matches_actual_managed_receipts_from_item_nine(gate, feed, expected):
    gate.run.run_id = 'acquisition_coverage__e28a43d__20261029'
    assert gate.ns['_expected_acquisition'](feed, gate.run) == expected
