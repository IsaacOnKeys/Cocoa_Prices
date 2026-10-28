"""Duplicate/revision validation executes only isolated versions of actual MERGEs."""

import runpy
from pathlib import Path

import pytest

MODULE = runpy.run_path(str(Path(__file__).resolve().parents[2] / 'scripts/diagnostics/validate_ingestion.py'))


@pytest.mark.parametrize('feed', ['cocoa', 'oil', 'weather'])
def test_only_isolated_tables_can_be_written_or_read(feed):
    destination, sql = MODULE['identity_merge_sql'](feed, '20261028_0123abcd')
    assert destination.startswith('cocoa-prices-430315.ingestion_validation_20261028_0123abcd.identity_curated_')
    assert 'cocoa_related.' not in sql and 'stream_staging.' not in sql
    assert 'ROW_NUMBER() OVER (PARTITION BY date ORDER BY ingestion_time DESC)' in sql


@pytest.mark.parametrize('token', ['production', '../20261028_0123abcd', '20261028_0123abcd`'])
def test_invalid_target_namespace_is_rejected(token):
    with pytest.raises(ValueError):
        MODULE['identity_merge_sql']('oil', token)
