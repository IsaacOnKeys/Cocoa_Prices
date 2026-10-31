"""Immutable backfill inputs, bounded execution and reviewed non-destructive repairs."""

from datetime import date
import hashlib
import importlib
import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def modules(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "pipelines/backfill"))
    contract = importlib.import_module("backfill_contract")
    spec = importlib.util.spec_from_file_location("history_repair_tool", ROOT / "scripts/operations/backfill_history.py")
    planner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(planner)
    return SimpleNamespace(contract=contract, planner=planner)


def request(module, **changes):
    fields = dict(feed="oil", input_uri="gs://raw-historic-data/snapshots/example.json", generation=123,
                  sha256="d" * 64, start="2014-01-01", end="2014-01-08",
                  dataset="cocoa-prices-430315.backfill_20261031_01234567", manifest_output="new-manifest.json")
    return module.BackfillRequest(**{**fields, **changes})


@pytest.mark.parametrize("changes", [{"dataset": "cocoa-prices-430315.cocoa_related"},
    {"input_uri": "gs://raw-historic-data/snapshots/*.json"}, {"generation": 0},
    {"input_uri": "gs://raw-historic-data/snapshots/../other.json"},
    {"start": "2013-01-01"}, {"start": "2014-1-1"}, {"end": "2013-12-31"}])
def test_unsafe_scope_is_rejected_before_cloud_access(modules, changes):
    with pytest.raises(ValueError):
        request(modules.contract, **changes)


@pytest.mark.parametrize("feed,sample", [("cocoa", "cocoa_prices.csv"), ("oil", "brent_oil_eia.json"), ("weather", "power_daily.csv")])
def test_real_format_headers_require_held_exact_generation_and_hash(modules, monkeypatch, feed, sample):
    data = (ROOT / "data/samples" / sample).read_bytes()
    blob = Mock(generation=123, temporary_hold=True, size=len(data))
    blob.download_as_bytes.return_value = data
    bucket = Mock()
    bucket.get_blob.return_value = blob
    bucket.blob.return_value = blob
    google, cloud, storage = (ModuleType(name) for name in ("google", "google.cloud", "google.cloud.storage"))
    google.__path__, cloud.__path__ = [], []
    storage.Client = Mock(return_value=SimpleNamespace(bucket=lambda _: bucket))
    cloud.storage = storage
    for module in (google, cloud, storage):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    config = request(modules.contract, feed=feed, sha256=hashlib.sha256(data).hexdigest())
    modules.contract.verify_source(config)
    blob.download_as_bytes.assert_called_once_with(if_generation_match=123, timeout=30)
    blob.temporary_hold = False
    with pytest.raises(ValueError, match="held"):
        modules.contract.verify_source(config)
    blob.temporary_hold, blob.generation = True, 124
    with pytest.raises(ValueError):
        modules.contract.verify_source(config)


def test_interval_selects_only_validated_requested_dates(modules):
    config = request(modules.contract)
    assert modules.contract.in_interval({"date": "2014-01-02"}, config)
    assert not modules.contract.in_interval({"date": "2014-01-09"}, config)
    assert not modules.contract.in_interval({"date": None}, config)


@pytest.mark.parametrize("state", ["CANCELLED", "FAILED", "RUNNING"])
def test_unsuccessful_runner_cannot_emit_completed_manifest(modules, state):
    with pytest.raises(RuntimeError, match="did not complete"):
        modules.contract.finish(request(modules.contract), SimpleNamespace(state=state))


def test_changed_code_cannot_issue_a_completed_manifest(modules):
    with pytest.raises(RuntimeError, match="code changed"):
        modules.contract.finish(request(modules.contract, transform_hashes={"unexpected": "d" * 64}), SimpleNamespace(state="DONE"))


@pytest.mark.parametrize("runner,allowed", [("DirectRunner", False), ("DataflowRunner", True), ("DataflowRunner", False)])
def test_expensive_runner_requires_opt_in_and_remains_bounded(modules, monkeypatch, tmp_path, runner, allowed):
    sdk = ModuleType("apache_beam.options.pipeline_options")
    classes = {name: type(name, (), {}) for name in ("DirectOptions", "GoogleCloudOptions", "StandardOptions", "WorkerOptions")}
    scope = {classes["StandardOptions"]: SimpleNamespace(runner=runner, streaming=False),
             classes["GoogleCloudOptions"]: SimpleNamespace(project=None, region=None),
             classes["DirectOptions"]: SimpleNamespace(), classes["WorkerOptions"]: SimpleNamespace()}
    for name, value in classes.items():
        setattr(sdk, name, value)
    sdk.PipelineOptions = lambda args: SimpleNamespace(view_as=lambda cls: scope[cls])
    beam, options = ModuleType("apache_beam"), ModuleType("apache_beam.options")
    beam.__path__, options.__path__ = [], []
    for module in (beam, options, sdk):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    verified = Mock()
    monkeypatch.setattr(modules.contract, "verify_source", verified)
    monkeypatch.setattr(modules.contract, "verify_destination", Mock())
    argv = ["--input", "gs://raw-historic-data/snapshots/example.json", "--input-generation", "123",
            "--input-sha256", "d" * 64, "--start-date", "2014-01-01", "--end-date", "2014-01-08",
            "--output-dataset", "cocoa-prices-430315.backfill_20261031_01234567", "--manifest-output", str(tmp_path / "new.json")]
    if allowed:
        argv.append("--allow-dataflow")
    if runner == "DataflowRunner" and not allowed:
        with pytest.raises(ValueError, match="explicit"):
            modules.contract.configure("oil", argv)
        verified.assert_not_called()
    else:
        modules.contract.configure("oil", argv)
        if runner == "DirectRunner":
            assert scope[classes["DirectOptions"]].direct_num_workers == 1
        else:
            worker = scope[classes["WorkerOptions"]]
            assert worker.num_workers == worker.max_num_workers == 1
            assert worker.disk_size_gb == 30 and worker.autoscaling_algorithm == "NONE"
            assert scope[classes["GoogleCloudOptions"]].dataflow_service_options == ["max_workflow_runtime_walltime_seconds=1800"]


def repair(modules, **changes):
    source = {"uri": "gs://raw-historic-data/snapshots/example.json", "generation": 123, "sha256": "d" * 64,
              "candidate_dataset": "cocoa-prices-430315.backfill_20261031_01234567", "acquired_at": None}
    args = dict(feed="oil", candidates=[{"date": "2014-01-02", "price": 11}, {"date": "2014-01-03", "price": None},
                                        {"date": "2014-01-04", "price": 30}],
                baseline=[{"date": "2014-01-02", "price": 10}, {"date": "2014-01-03", "price": 20}],
                start="2014-01-02", end="2014-01-04", source=source, target=modules.planner.context(),
                today=date(2026, 10, 31), prepared_at="2026-10-31T00:00:00+00:00")
    return modules.planner.build_plan(**{**args, **changes})


def test_review_plan_distinguishes_insert_correction_and_preserved_null(modules):
    plan = repair(modules)
    assert plan["summary"] == {"inserted_dates": 1, "changed_dates": 1, "unchanged_dates": 1, "preserved_missing_measurements": 1}
    assert plan["rows"][1]["before"]["price"] == 20 and plan["rows"][1]["proposed"]["price"] is None
    modules.planner.validate_plan(plan)
    original = modules.planner.identity(plan)
    plan["rows"][0]["proposed"]["price"] = 12
    assert modules.planner.identity(plan) != original
    with pytest.raises(ValueError, match="inconsistent"):
        modules.planner.validate_plan(plan)


@pytest.mark.parametrize("changes", [
    {"candidates": [{"date": "2014-01-02", "price": 1}, {"date": "2014-01-02", "price": 2}]},
    {"candidates": [{"date": "2014-01-05", "price": 1}]},
    {"candidates": [{"date": "2014-01-02", "price": True}]},
    {"candidates": [{"date": "2014-01-02", "price": float("inf")}]},
    {"start": "2026-10-01", "end": "2026-10-02"},
    {"target": {"delivery": "analytics_delivery", "curated": "backfill_baseline_20261031_01234567"}},
])
def test_ambiguous_recent_or_mixed_repair_is_rejected(modules, changes):
    with pytest.raises(ValueError):
        repair(modules, **changes)
