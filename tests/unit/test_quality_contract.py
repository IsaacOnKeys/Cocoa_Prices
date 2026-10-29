"""Quality evidence preserves actual source revisions and restricts destructive probes."""

import json
import runpy
from pathlib import Path

import pytest

from test_required_feed_completion import gate as gate_fixture
from test_required_feed_completion import install, poke, receipt

ROOT=Path(__file__).resolve().parents[2]
PROBE=runpy.run_path(str(ROOT/'scripts/diagnostics/validate_quality.py'))


def test_market_inventory_never_becomes_an_invented_daily_calendar(monkeypatch):
    gate=gate_fixture.__wrapped__(monkeypatch)
    document=receipt(gate,feed='oil')
    install(gate,document,feed='oil')
    assert poke(gate,feed='oil') is True
    accepted=gate.ti.xcom_push.call_args.kwargs['value']
    assert accepted['observation_manifest']==document['observations']
    assert len(accepted['observation_manifest'])==accepted['observations']==2
    assert len(document['not_exposed_dates'])==29


def test_explicit_source_missingness_survives_into_the_quality_contract(monkeypatch):
    gate=gate_fixture.__wrapped__(monkeypatch)
    document=receipt(gate,missing_days=4)
    install(gate,document)
    assert poke(gate) is True
    accepted=gate.ti.xcom_push.call_args.kwargs['value']
    assert accepted['observation_manifest']==document['observations']
    assert accepted['observation_manifest'][-1]['missing_measurements']==['precipitation','soil_moisture']


@pytest.mark.parametrize('token',['production','20261029_12345678/extra','20261029_ABCDEF12'])
def test_quality_probe_rejects_non_isolated_names(token):
    with pytest.raises(ValueError):
        PROBE['names'](token)


def test_quality_probe_refuses_a_tampered_cleanup_destination(tmp_path):
    path=tmp_path/'receipt.json'
    path.write_text(json.dumps({'token':'20261029_12345678','dataset':'cocoa-prices-430315.cocoa_related'}))
    with pytest.raises(ValueError,match='isolated'):
        PROBE['checked'](path)
