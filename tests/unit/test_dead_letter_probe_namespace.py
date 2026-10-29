"""The mutating diagnostic can only clean up its isolated namespace."""

import json
import runpy
from pathlib import Path

import pytest

M = runpy.run_path(str(Path(__file__).resolve().parents[2] / 'scripts/diagnostics/validate_dead_letter_recovery.py'))


def test_probe_names_are_bounded_and_distinct_from_production():
    names = M['names']('20261029_0123abcd')
    assert len(set(names.values())) == 5
    assert all('recovery-validation-20261029-0123abcd' in name for name in names.values())
    assert len(names['bucket']) <= 63


@pytest.mark.parametrize('token', ['', '../production', '20261029_0123abcd/extra', '20261029_0123abcde'])
def test_reject_non_probe_names(token):
    with pytest.raises(ValueError):
        M['names'](token)


def test_refuse_tampered_cleanup_names(tmp_path):
    token = '20261029_0123abcd'
    names = M['names'](token)
    names['bucket'] = 'raw-historic-data'
    receipt = tmp_path / 'probe.json'
    receipt.write_text(json.dumps({'token': token, 'names': names}))
    with pytest.raises(ValueError, match='isolated'):
        M['checked'](receipt)
