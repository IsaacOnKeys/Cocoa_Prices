"""Integration cleanup must be unable to address production or unrelated resources."""

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("validate_ingestion", ROOT / "scripts/diagnostics/validate_ingestion.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


@pytest.mark.parametrize("invalid", [None, "dataset", "subscription", "token", "extra_feed"])
def test_namespace_guard(tmp_path, invalid):
    token = "20261027_0123abcd"
    state = {"token": token, "dataset": f"{MODULE.PROJECT}.ingestion_validation_{token}",
             "subscriptions": {feed: f"projects/{MODULE.PROJECT}/subscriptions/ingestion-validation-{token.replace('_','-')}-{feed}" for feed in MODULE.TABLES}}
    if invalid == "dataset":
        state["dataset"] = f"{MODULE.PROJECT}.stream_staging"
    if invalid == "subscription":
        state["subscriptions"]["oil"] = f"projects/{MODULE.PROJECT}/subscriptions/oil-prices-sub"
    if invalid == "token":
        state["token"] = "../../production"
    if invalid == "extra_feed":
        state["subscriptions"]["other"] = "unrelated-resource"
    path = tmp_path / "state.json"
    path.write_text(json.dumps(state))
    if invalid:
        with pytest.raises(ValueError):
            MODULE.checked_state(path)
    else:
        assert MODULE.checked_state(path) == state


def test_verify_waits_for_delayed_acceptance_log(tmp_path, monkeypatch):
    token = "20261027_0123abcd"
    state = {"token": token, "dataset": f"{MODULE.PROJECT}.ingestion_validation_{token}",
             "subscriptions": {feed: f"projects/{MODULE.PROJECT}/subscriptions/ingestion-validation-{token.replace('_','-')}-{feed}" for feed in MODULE.TABLES},
             "initial_delivery_id": "a" * 64, "vm_stopped": True,
             "unauthenticated_denied": True, "initial_sink_rows": 0}
    path = tmp_path / "state.json"
    path.write_text(json.dumps(state))
    class Client:
        def query(self, sql):
            feed = next(feed for feed, table in MODULE.TABLES.items() if f".{table}`" in sql)
            rows = [{"date": date, "raw_payload": json.dumps({"validation": token}),
                     **{name: value if date == "2026-10-20" else None for name, value in MODULE.MEASUREMENTS[feed].items()}}
                    for date in ("2026-10-20", "2026-10-21")]
            return SimpleNamespace(result=lambda: rows)
    fake_bq = ModuleType("google.cloud.bigquery")
    fake_bq.Client = lambda **kwargs: Client()
    google = ModuleType("google")
    cloud = ModuleType("google.cloud")
    google.cloud = cloud
    cloud.bigquery = fake_bq
    for name, module in (("google", google), ("google.cloud", cloud), ("google.cloud.bigquery", fake_bq)):
        monkeypatch.setitem(sys.modules, name, module)
    retry = {"event": "ingestion_retry", "error_type": "HTTPError"}
    observations = iter([[retry], [retry, {"event": "ingestion_accepted"}]])
    monkeypatch.setattr(MODULE, "read_logs", lambda *args: next(observations))
    pauses = []
    monkeypatch.setattr(MODULE.time, "sleep", pauses.append)
    MODULE.verify(path, object())
    assert pauses == [5]
    assert json.loads(path.read_text())["passed"] is True


@pytest.mark.parametrize("invalid", [None, "production_table", "wrong_feed", "extra_route"])
def test_route_guard_checks_destinations_not_only_subscription_names(invalid):
    state = {"dataset": f"{MODULE.PROJECT}.ingestion_validation_20261027_0123abcd",
             "subscriptions": {"oil": f"projects/{MODULE.PROJECT}/subscriptions/ingestion-validation-20261027-0123abcd-oil"}}
    subscription = state["subscriptions"]["oil"]
    configured = {subscription: {"feed": "oil", "table": f"{state['dataset']}.oil_prices"}}
    if invalid == "production_table":
        configured[subscription]["table"] = f"{MODULE.PROJECT}.stream_staging.oil_prices"
    if invalid == "wrong_feed":
        configured[subscription]["feed"] = "cocoa"
    if invalid == "extra_route":
        configured["unrelated"] = configured[subscription]
    if invalid:
        with pytest.raises(ValueError):
            MODULE.check_routes(configured, state)
    else:
        MODULE.check_routes(configured, state)


def test_isolated_routes_can_coexist_with_exact_unchanged_production_routes():
    state = {"dataset": f"{MODULE.PROJECT}.ingestion_validation_20261028_0123abcd",
             "subscriptions": {"oil": f"projects/{MODULE.PROJECT}/subscriptions/ingestion-validation-20261028-0123abcd-oil"}}
    configured = {state["subscriptions"]["oil"]: {"feed": "oil", "table": state["dataset"]+".oil_prices"}}
    configured.update({
        f"projects/{MODULE.PROJECT}/subscriptions/{subscription}": {"feed": feed, "table": f"{MODULE.PROJECT}.stream_staging.{MODULE.TABLES[feed]}"}
        for feed, subscription in (("cocoa", "cocoa-prices-sub"), ("oil", "oil-prices-sub"), ("weather", "weather-data-sub"))
    })
    MODULE.check_routes(configured, state)
    configured[f"projects/{MODULE.PROJECT}/subscriptions/oil-prices-sub"]["table"] = state["dataset"]+".oil_prices"
    with pytest.raises(ValueError):
        MODULE.check_routes(configured, state)
