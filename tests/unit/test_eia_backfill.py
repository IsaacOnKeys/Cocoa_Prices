"""Validate EIA archive interpretation without importing Beam IO or executing a job."""

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def cleaner(monkeypatch):
    beam = ModuleType("apache_beam")
    beam.__path__ = []
    beam.DoFn = object
    beam.pvalue = SimpleNamespace(TaggedOutput=lambda tag, record: (tag, record))
    options = ModuleType("apache_beam.options")
    options.__path__ = []
    pipeline_options = ModuleType("apache_beam.options.pipeline_options")
    pipeline_options.GoogleCloudOptions = object
    pipeline_options.StandardOptions = object
    pipeline_options.PipelineOptions = lambda: SimpleNamespace(
        view_as=lambda _: SimpleNamespace(runner="DirectRunner")
    )
    for module in (beam, options, pipeline_options):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    spec = importlib.util.spec_from_file_location("eia_cleaner", ROOT / "pipelines/backfill/clean_oil_eia.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 27, tzinfo=timezone.utc).astimezone(tz)

    module.datetime = FixedDateTime
    return module


def archive(**changes):
    return {
        "series": "RBRTE", "units": "$/BBL", "acquisition_interface": "published-table",
        "response": {"data": [{"period": "2026-10-20", "value": "115.22", "series": "RBRTE", "units": "$/BBL"}]},
        **changes,
    }


def test_accepted_archive_maps_correct_date_units_and_current_year(cleaner):
    records = list(cleaner.extract_and_clean(archive()))
    assert records == [{"date": "2026-10-20", "brent_price_eu": 115.22}]
    assert list(cleaner.ValidateAndTransform().process(records[0])) == records


@pytest.mark.parametrize("changes", [
    {"series": "RWTC"}, {"units": "EUR/barrel"}, {"acquisition_interface": "api"},
])
def test_unapproved_source_archive_fails_instead_of_loading(cleaner, changes):
    with pytest.raises(ValueError, match="accepted EIA published-table archive"):
        list(cleaner.extract_and_clean(archive(**changes)))


@pytest.mark.parametrize("price", [float("nan"), float("inf"), None])
def test_invalid_prices_are_tagged_for_invalid_output(cleaner, price):
    result = list(cleaner.ValidateAndTransform().process({"date": "2026-10-20", "brent_price_eu": price}))
    assert result[0][0] == "invalid"
    assert result[0][1]["Errors"]
