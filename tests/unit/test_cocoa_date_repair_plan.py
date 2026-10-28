"""Repair candidates replace unverified date labels without fabricating dates or rewriting unaffected history."""

import copy
import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("cocoa_repair_plan", ROOT / "scripts/operations/plan_cocoa_date_repair.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def inputs():
    staging = [{"date": "2026-10-26", "cocoa_price": "5075.02", "ingestion_time": "2026-10-26T18:00:00Z", "raw_payload": '{"date":"2026-10-26","price":5075.02}'},
               {"date": "2026-10-27", "cocoa_price": "5075.02", "ingestion_time": "2026-10-27T18:00:00Z", "raw_payload": '{"date":"2026-10-27","price":5075.02}'}]
    curated = [{"date": "2024-01-02", "cocoa_price": "3000"}, {"date": "2026-10-26", "cocoa_price": "5075.02"},
               {"date": "2026-10-27", "cocoa_price": "5075.02"}]
    publisher = {"publisher": "ICCO", "source_url": "https://www.icco.org/statistics/", "units": "EUR/metric_tonne", "acquired_at": "2026-10-28T08:00:00+00:00",
                 "observations": [{"date": "2026-10-23", "cocoa_price": 5075.02}, {"date": "2026-10-27", "cocoa_price": 5121.21}]}
    return staging, curated, publisher


def test_rebuild_uses_only_dated_source_preserves_before_period_and_keeps_acquisition_separate():
    staging, curated, publisher = inputs()
    originals = copy.deepcopy((staging, curated, publisher))
    plan = MODULE.build_plan(staging, curated, publisher, archive_reference="gs://private/verified-source.json#123")
    assert (staging, curated, publisher) == originals
    assert plan["removed_dates"] == ["2026-10-26"]
    assert plan["changed_dates"] == ["2026-10-27"]
    assert plan["candidate_curated"] == [{"date": "2024-01-02", "cocoa_price": 3000.0}, {"date": "2026-10-27", "cocoa_price": 5121.21}]
    row = plan["candidate_staging"][0]
    assert row["date"] == "2026-10-27" and row["ingestion_time"] == "2026-10-28T08:00:00+00:00"
    assert json.loads(row["raw_payload"])["date_basis"] == "publisher_dated_history"
    assert plan["summary"]["legacy_rows_to_preserve_and_quarantine"] == 2


@pytest.mark.parametrize("defect", ["units", "timezone", "start_coverage", "conflict", "nonfinite", "future", "corrected_row", "curated_duplicate"])
def test_ambiguous_or_incompatible_repair_inputs_fail_closed(defect):
    staging, curated, publisher = inputs()
    if defect == "units":
        publisher["units"] = "USD/tonne"
    if defect == "timezone":
        publisher["acquired_at"] = "2026-10-28T08:00:00"
    if defect == "start_coverage":
        publisher["observations"] = [{"date": "2026-10-27", "cocoa_price": 5121.21}]
    if defect == "conflict":
        publisher["observations"].append({"date": "2026-10-27", "cocoa_price": 5000})
    if defect == "nonfinite":
        publisher["observations"][0]["cocoa_price"] = float("nan")
    if defect == "future":
        publisher["observations"].append({"date": "2026-10-29", "cocoa_price": 5000})
    if defect == "corrected_row":
        staging[0]["raw_payload"] = '{"date_basis":"source_observation"}'
    if defect == "curated_duplicate":
        curated.append(curated[0])
    with pytest.raises(ValueError):
        MODULE.build_plan(staging, curated, publisher, archive_reference="gs://private/source")


@pytest.mark.parametrize("defect", [None, "mixed_destinations", "cross_repair", "unexpected_placeholder"])
def test_sql_binding_is_scoped_and_preserves_json_paths(defect):
    namespace = "cocoa-prices-430315.cocoa_date_repair_20261028_0123abcd"
    names = {key: f"{namespace}.{suffix}" for key, suffix in {
        "ACTIVE_STAGING": "rehearsal_staging", "ACTIVE_CURATED": "rehearsal_curated",
        "STAGING_BEFORE": "staging_before", "CURATED_BEFORE": "curated_before",
        "CANDIDATE_STAGING": "candidate_staging", "CANDIDATE_CURATED": "candidate_curated",
        "LEGACY_ARCHIVE": "legacy_observations"}.items()}
    sql = (ROOT / "sql/repairs/cocoa_observation_dates.sql").read_text()
    if defect == "mixed_destinations":
        names["ACTIVE_STAGING"] = "cocoa-prices-430315.stream_staging.cocoa_prices"
    if defect == "cross_repair":
        names["STAGING_BEFORE"] = "cocoa-prices-430315.stream_staging.cocoa_prices"
    if defect == "unexpected_placeholder":
        sql += "\nSELECT '${UNKNOWN}';"
    if defect:
        with pytest.raises(ValueError):
            MODULE.render_transaction(sql, names)
    else:
        rendered = MODULE.render_transaction(sql, names)
        assert "$.date_basis" in rendered and "${" not in rendered
        assert "stream_staging.cocoa_prices" not in rendered
