"""Transport retries and logical retries retain identity; source revisions differ."""

import runpy
from pathlib import Path
from types import SimpleNamespace

import pytest

POLICY = runpy.run_path(str(Path(__file__).resolve().parents[2] / 'functions/shared/request_policy.py'))


def test_logical_key_survives_a_new_trigger_message_id():
    event = {'attributes': {'acquisition_key': 'dag:run:oil'}}
    first = SimpleNamespace(event_id='123', resource={'name': 'projects/unit/topics/oil'})
    second = SimpleNamespace(event_id='456', resource={'name': 'projects/unit/topics/oil'})
    identity = POLICY['acquisition_identity']
    assert identity('EIA', event=event, context=first) == identity('EIA', event=event, context=second)
    assert identity('EIA', context=first) != identity('EIA', context=second)
    assert identity('EIA', context=first) != identity('NASA_POWER', context=first)


def test_event_source_and_id_are_both_significant():
    first = SimpleNamespace(event_id='123', resource={'name': 'projects/unit/topics/a'})
    second = SimpleNamespace(event_id='123', resource={'name': 'projects/unit/topics/b'})
    assert POLICY['acquisition_identity']('EIA', context=first) != POLICY['acquisition_identity']('EIA', context=second)


@pytest.mark.parametrize('key', ['', 123, 'x' * 1025])
def test_invalid_caller_key_is_rejected(key):
    with pytest.raises(RuntimeError, match='Invalid acquisition identity'):
        POLICY['acquisition_identity']('ICCO', key=key)


def test_observation_hash_ignores_mapping_order_but_changes_for_source_revisions():
    identity = POLICY['observation_identity']
    assert identity('NASA_POWER', '2026-10-28', {'a': 1., 'b': None}) == identity('NASA_POWER', '2026-10-28', {'b': None, 'a': 1.})
    assert identity('EIA', '2026-10-28', {'oil_price': 10.}) != identity('EIA', '2026-10-28', {'oil_price': 11.})
