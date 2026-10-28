"""The isolated delivery validator cannot be redirected to production resources."""

import base64
import json
import runpy
from pathlib import Path

import pytest

MODULE = runpy.run_path(str(Path(__file__).resolve().parents[2] / 'scripts/diagnostics/validate_delivery.py'))
TOKEN = '20261028_0123abcd'


@pytest.mark.parametrize('token', [None, '', 'production', '../20261028_0123abcd', '20261028_0123abcd/other'])
def test_invalid_namespaces_are_rejected(token):
    with pytest.raises(ValueError):
        MODULE['names'](token)


def test_altered_resource_names_are_rejected(tmp_path):
    state = {'token': TOKEN, 'names': MODULE['names'](TOKEN)}
    state['names']['topic'] = 'eia-oil-trigger'
    path = tmp_path / 'receipt.json'
    path.write_text(json.dumps(state))
    with pytest.raises(ValueError, match='altered'):
        MODULE['checked'](path)


def test_source_cannot_be_redirected_to_another_bucket(tmp_path):
    state = {'token': TOKEN, 'names': MODULE['names'](TOKEN),
             'source': {'bucket': 'raw-historic-data', 'object': 'anything', 'sha256': 'a' * 64, 'generation': '123'}}
    path = tmp_path / 'receipt.json'
    path.write_text(json.dumps(state))
    with pytest.raises(ValueError, match='source reference'):
        MODULE['checked'](path)


def test_forwarded_payload_verification_requires_original_bytes():
    original = b'{"scenario":"permanent"}'
    check = MODULE['original_payload_matches']
    assert check(original, original)
    assert check(json.dumps({'message': {'data': base64.b64encode(original).decode()}}).encode(), original)
    assert not check(b'other', original)


def test_embedded_probe_compiles_on_the_test_interpreter():
    compile(MODULE['PROBE'], 'delivery_probe', 'exec')
