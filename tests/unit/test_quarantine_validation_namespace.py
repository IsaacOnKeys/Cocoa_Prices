"""Managed quarantine cleanup cannot target production or altered resources."""

import json
import runpy
from types import SimpleNamespace
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def validator(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT/'scripts/diagnostics'))
    return runpy.run_path(str(ROOT/'scripts/diagnostics/validate_quarantine.py'))


@pytest.mark.parametrize('token', ['production', '../escape', '20261029_deadbeef/extra'])
def test_invalid_namespace_rejected(validator, token):
    with pytest.raises(ValueError):
        validator['names'](token)


@pytest.mark.parametrize('tamper', ['bucket', 'dataset', 'function', 'topic', 'source'])
def test_changed_destinations_rejected_before_network(validator, tmp_path, tamper):
    token = '20261029_deadbeef'
    state = {'token': token, 'names': validator['names'](token)}
    if tamper == 'topic':
        state['names']['topics']['oil'] = 'oil-prices-topic'
    elif tamper == 'source':
        state['source'] = {'bucket': 'raw-historic-data', 'object': 'not-validation', 'sha256': 'a'*64, 'generation': '1'}
    else:
        state['names'][tamper] = 'production'
    path = tmp_path/'state.json'
    path.write_text(json.dumps(state))
    with pytest.raises(ValueError):
        validator['checked'](path)


def test_new_conditional_binding_explicitly_uses_policy_version_three(validator):
    policy = SimpleNamespace(version=1, bindings=[])
    written = []
    bucket = SimpleNamespace(name='isolated-bucket', get_iam_policy=lambda **kwargs: policy,
                             set_iam_policy=lambda value, **kwargs: written.append(value))
    validator['storage_access'](bucket, True)
    assert written == [policy] and policy.version == 3
    assert {b['role'] for b in policy.bindings} == {'roles/storage.objectCreator', 'roles/storage.objectViewer'}
    assert all(b['condition']['expression'] == "resource.name.startsWith('projects/_/buckets/isolated-bucket/objects/quarantine/v1/')" for b in policy.bindings)
