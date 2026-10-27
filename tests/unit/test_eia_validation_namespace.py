"""Prevent the integration utility from selecting production warehouse tables."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("eia_route_validation", ROOT / "scripts/diagnostics/validate_eia_route.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_own_isolated_namespace_is_allowed():
    MODULE.check_table("cocoa-prices-430315.cocoa_eia_validation_20261027_ab123456.oil_prices")


@pytest.mark.parametrize("table", [
    "cocoa-prices-430315.stream_staging.oil_prices",
    "cocoa-prices-430315.cocoa_related.brent_prices",
    "other-project.cocoa_eia_validation_test.oil_prices",
    "cocoa-prices-430315.cocoa_eia_validation_test.other_table",
    "cocoa-prices-430315.cocoa_eia_validation_test.oil_prices` WHERE TRUE",
])
def test_production_or_unexpected_namespace_is_rejected(table):
    with pytest.raises(ValueError, match="isolated EIA validation"):
        MODULE.check_table(table)
