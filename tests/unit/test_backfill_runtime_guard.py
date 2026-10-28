"""Native-runner validation must never address a production sink."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("backfill_runtime_guard", ROOT / "scripts/diagnostics/validate_backfill_runtime.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


@pytest.mark.parametrize("dataset", [
    "cocoa-prices-430315.cocoa_related", "other.runtime_validation_20261028_0123abcd",
    "cocoa-prices-430315.runtime_validation_../../production", "cocoa-prices-430315.runtime_validation_20261028_0123abcd.more",
])
def test_production_and_unrelated_destinations_are_rejected(dataset):
    with pytest.raises(ValueError):
        MODULE.mapped_table("cocoa-prices-430315:cocoa_related.cocoa", "cocoa", dataset)


def test_expected_sink_can_only_be_rebound_to_the_controlled_namespace():
    dataset = "cocoa-prices-430315.runtime_validation_20261028_0123abcd"
    assert MODULE.mapped_table("cocoa-prices-430315:cocoa_related.cocoa", "cocoa", dataset) == dataset.replace(".", ":", 1)+".cocoa"
    with pytest.raises(ValueError):
        MODULE.mapped_table("cocoa-prices-430315:cocoa_related.brent_prices", "cocoa", dataset)
