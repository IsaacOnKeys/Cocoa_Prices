"""Validation cleanup accepts only its derived disposable namespace."""

import json
import runpy
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def validator(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT/'scripts/diagnostics'))
    return runpy.run_path(str(ROOT/'scripts/diagnostics/validate_coverage.py'))


@pytest.mark.parametrize('token', ['production', '../escape', '20261029_deadbeef/extra'])
def test_invalid_namespace_is_rejected(validator, token):
    with pytest.raises(ValueError):
        validator['names'](token)


@pytest.mark.parametrize('tamper', ['bucket', 'topic', 'function', 'source'])
def test_cleanup_rejects_changed_resource_names(validator, tmp_path, tamper):
    token = '20261029_deadbeef'
    state = {'token': token, 'names': validator['names'](token)}
    if tamper == 'bucket':
        state['names']['buckets']['state'] = 'raw-historic-data'
    elif tamper == 'topic':
        state['names']['topics']['cocoa'] = 'cocoa-prices-topic'
    elif tamper == 'function':
        state['names']['function'] = 'publish-price'
    else:
        state['source'] = {'bucket': 'raw-historic-data', 'object': 'not-validation', 'sha256': 'a'*64, 'generation': '1'}
    path = tmp_path/'state.json'
    path.write_text(json.dumps(state))
    with pytest.raises(ValueError):
        validator['checked'](path)
