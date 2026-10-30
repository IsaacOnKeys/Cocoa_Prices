"""SQL deployment identifiers cannot escape or conflate the isolated target datasets."""

import runpy
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
M = runpy.run_path(str(ROOT / 'scripts/deploy/render_delivery_sql.py'))
PROBE = runpy.run_path(str(ROOT / 'scripts/diagnostics/validate_atomic_delivery.py'))


@pytest.mark.parametrize('kind', ['migration', 'procedure', 'rollback', 'bootstrap', 'serving_views', 'retire_legacy',
                                'input_quality_checks', 'output_quality_checks', 'imputation_join',
                                'imputation_view', 'revision_selection'])
def test_isolated_sql_contains_no_production_data_destinations(kind):
    context = PROBE['names']('20261029_0123abcd')
    sql = M['render'](kind, **context)
    assert '${' not in sql
    assert 'cocoa-prices-430315.cocoa_related.' not in sql
    assert 'cocoa-prices-430315.stream_staging.' not in sql
    assert 'cocoa-prices-430315.analytics_delivery.' not in sql


@pytest.mark.parametrize('values', [dict(project='valid-project`;.invalid'),
                                   dict(delivery='analytics_delivery; DROP TABLE x'),
                                   dict(curated='analytics_delivery'), dict(staging='cocoa_related'),
                                   dict(unexpected='injected')])
def test_identifier_injection_or_dataset_collision_fails_closed(values):
    with pytest.raises(ValueError):
        M['render']('procedure', **values)


def test_validation_namespace_rejects_production_or_path_suffix():
    for token in ['analytics_delivery', '20261029_0123abcd/production', '20261029_0123abcde']:
        with pytest.raises(ValueError):
            PROBE['names'](token)
