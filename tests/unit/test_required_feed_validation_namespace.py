"""Guard destructive fixture cleanup and delayed-row writes against production."""

import json
import runpy
from pathlib import Path

import pytest


@pytest.fixture
def validator(monkeypatch):
    root = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(root/'scripts/diagnostics'))
    return runpy.run_path(str(root/'scripts/diagnostics/validate_required_feeds.py'))


@pytest.mark.parametrize('token', ['production', '../escape', '20261029_deadbeef/other'])
def test_invalid_namespace_is_rejected(validator, token):
    with pytest.raises(ValueError):
        validator['names'](token)


@pytest.mark.parametrize('target', ['dataset', 'bucket', 'run'])
def test_changed_validation_destinations_are_rejected(validator, tmp_path, target):
    token = '20261029_deadbeef'
    state = {'token': token, 'names': validator['names'](token), 'runs': {
        case: {'run_id': 'required_feed_validation__'+token+'__'+case} for case in validator['CASES']}}
    if target == 'dataset':
        state['names']['dataset'] = 'cocoa-prices-430315.cocoa_related'
    elif target == 'bucket':
        state['names']['bucket'] = 'raw-historic-data'
    else:
        state['runs']['ready']['run_id'] = 'production'
    path = tmp_path/'state.json'
    path.write_text(json.dumps(state))
    with pytest.raises(ValueError):
        validator['checked'](path)
