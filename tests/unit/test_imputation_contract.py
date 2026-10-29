"""The destructive native imputation diagnostic only accepts its exact namespace."""

import json
import runpy
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PROBE = runpy.run_path(str(ROOT/'scripts/diagnostics/validate_imputation.py'))


@pytest.mark.parametrize('token',['production','20261029_12345678/extra','20261029_ABCDEF12'])
def test_imputation_probe_rejects_non_isolated_names(token):
    with pytest.raises(ValueError):
        PROBE['names'](token)


def test_imputation_cleanup_refuses_a_tampered_destination(tmp_path):
    path = tmp_path/'receipt.json'
    path.write_text(json.dumps({'token':'20261029_12345678','dataset':'cocoa-prices-430315.cocoa_related'}))
    with pytest.raises(ValueError,match='isolated'):
        PROBE['checked'](path)
