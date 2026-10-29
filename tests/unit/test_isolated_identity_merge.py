"""The retired MERGE diagnostic cannot bypass the new release publication path."""

import runpy
from pathlib import Path

import pytest

MODULE = runpy.run_path(str(Path(__file__).resolve().parents[2] / 'scripts/diagnostics/validate_ingestion.py'))


@pytest.mark.parametrize('feed', ['cocoa', 'oil', 'weather'])
def test_legacy_merge_cannot_generate_publication_sql(feed):
    with pytest.raises(RuntimeError, match='validate_atomic_delivery.py'):
        MODULE['identity_merge_sql'](feed, '20261028_0123abcd')


@pytest.mark.parametrize('token', ['production', '../20261028_0123abcd', '20261028_0123abcd`'])
def test_invalid_target_namespace_is_rejected(token):
    with pytest.raises(ValueError):
        MODULE['identity_merge_sql']('oil', token)


def test_retired_exercise_stops_before_reading_state_or_creating_clients():
    with pytest.raises(RuntimeError, match='retired'):
        MODULE['exercise_identities'](None, None)
