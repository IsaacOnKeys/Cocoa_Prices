"""A release uses committed bytes and detects artifact corruption before deployment."""

import importlib.util
import json
from pathlib import Path
from zipfile import ZipFile

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('application_release', ROOT / 'scripts/deploy/package_release.py')
RELEASE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RELEASE)


@pytest.fixture
def candidate(tmp_path):
    folder = tmp_path / 'candidate'
    manifest = RELEASE.build(folder, 'HEAD', 'application_validation_20261031_1234abcd')
    return folder, manifest


def test_committed_candidate_covers_runtime_and_isolated_sql(candidate):
    folder, manifest = candidate
    assert len(manifest['git_revision']) == 40 and manifest['status'] == 'candidate_not_deployed'
    with ZipFile(folder / 'source.zip') as source:
        assert set(source.namelist()).issuperset({'airflow/Dockerfile', 'airflow/dags/cocoa_daily.py',
            'pipelines/backfill/backfill_contract.py', 'sql/migrations/029_historical_repairs.sql'})
        assert not any(name.endswith(('.tfvars', '.tfstate', '.env', '.json')) for name in source.namelist())
    for feed in ('cocoa', 'oil', 'weather', 'ingestion', 'monitor'):
        with ZipFile(folder / 'functions' / (feed + '.zip')) as function:
            assert 'constraints-python311.txt' in function.namelist()
    sql = (folder / 'isolated-sql/procedure.sql').read_text()
    assert 'application_validation_20261031_1234abcd' in sql
    assert 'cocoa-prices-430315.analytics_delivery.' not in sql
    assert RELEASE.verify(folder) == manifest
    repeated = RELEASE.build(folder.parent / 'repeat', manifest['git_revision'], 'application_validation_20261031_1234abcd')
    assert repeated == manifest
    # Verification must reject corruption before any operator applies SQL or source.
    (folder / 'functions/ingestion.zip').write_bytes(b'tampered')
    with pytest.raises(ValueError, match='checksum'):
        RELEASE.verify(folder)


def test_path_escape_in_manifest_is_rejected(candidate):
    folder, manifest = candidate
    manifest['files']['../outside'] = 'a' * 64
    (folder / 'manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='Unsafe'):
        RELEASE.verify(folder)


def test_production_namespace_and_existing_output_are_rejected(tmp_path):
    with pytest.raises(ValueError, match='isolated'):
        RELEASE.build(tmp_path / 'release', 'HEAD', 'analytics_delivery')
    with pytest.raises(ValueError, match='must not exist'):
        RELEASE.build(tmp_path, 'HEAD', 'application_validation_20261031_1234abcd')


@pytest.mark.parametrize('path', ['../escape', '/absolute', 'C:/escape', 'C:escape', 'folder\\escape'])
def test_external_paths_are_rejected(path):
    with pytest.raises(ValueError, match='Unsafe'):
        RELEASE.relative(path)
