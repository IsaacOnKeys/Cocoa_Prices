"""Revision fingerprints cross the actual Avro boundary; duplicate order has no authority."""

import copy
import itertools
import runpy
from pathlib import Path

import pytest

from test_ingestion import consumer as consumer_fixture, envelope, request, MEASUREMENTS
from test_ingestion_quarantine import storage as storage_fixture, stored
from test_backfill_recovery import cleaners as cleaners_fixture

ROOT = Path(__file__).resolve().parents[2]
IDENTITY = runpy.run_path(str(ROOT / "functions/shared/request_policy.py"))[
    "observation_identity"
]
SOURCES = {"cocoa": "ICCO", "oil": "EIA", "weather": "NASA_POWER"}


@pytest.fixture
def consumer(monkeypatch, tmp_path):
    return consumer_fixture.__wrapped__(monkeypatch, tmp_path)


@pytest.fixture
def storage(consumer, monkeypatch):
    return storage_fixture.__wrapped__(consumer, monkeypatch)


@pytest.fixture
def cleaners(monkeypatch):
    return cleaners_fixture.__wrapped__(monkeypatch)


@pytest.mark.parametrize("feed", SOURCES)
def test_publisher_fingerprint_survives_wire_decoding_and_transport_changes(
    consumer, storage, feed
):
    values = MEASUREMENTS[feed]
    fingerprint = IDENTITY(SOURCES[feed], "2026-10-20", values)
    body = envelope(feed)
    body["message"]["attributes"].update(
        acquisition_id="a" * 64, observation_id=fingerprint
    )
    assert consumer.stage_analytics_data(request(body))[1] == 204
    body["message"]["messageId"] = "123456790"
    assert consumer.stage_analytics_data(request(body))[1] == 204
    assert storage.sink.call_count == 2 and not storage.objects
    assert all(
        c.args[1]["observation_id"] == fingerprint for c in storage.sink.call_args_list
    )


@pytest.mark.parametrize("feed", SOURCES)
def test_changed_business_value_cannot_reuse_an_accepted_fingerprint(
    consumer, storage, feed
):
    values = dict(MEASUREMENTS[feed])
    fingerprint = IDENTITY(SOURCES[feed], "2026-10-20", values)
    field = next(iter(values))
    values[field] += 1.0
    record = {
        "date": "2026-10-20",
        **values,
        "ingestion_time": "2026-10-20T10:15:30Z",
        "raw_payload": "fixture",
    }
    body = envelope(feed, record)
    body["message"]["attributes"].update(
        acquisition_id="a" * 64, observation_id=fingerprint
    )
    assert consumer.stage_analytics_data(request(body))[1] == 204
    storage.sink.assert_not_called()
    evidence, _ = stored(storage)
    assert evidence["error_details"]["code"] == "observation_identity_mismatch"


@pytest.mark.parametrize("feed", SOURCES)
@pytest.mark.parametrize("conflict", [False, True])
def test_historical_duplicate_resolution_is_independent_of_input_order(
    cleaners, feed, conflict
):
    module = cleaners[feed]
    fields = {
        "cocoa": {"Date": "2026-10-20", "Euro_Price": 6500.25},
        "oil": {"date": "2026-10-20", "brent_price_eu": 72.5},
        "weather": {"date": "2026-10-20", "precipitation": 3.5, "soil_moisture": 0.42},
    }[feed]
    rows = [dict(fields), dict(fields), dict(fields)]
    if conflict:
        rows[-1][next(k for k in fields if k not in ("date", "Date"))] += 1.0
    original = copy.deepcopy(rows)
    outputs = []
    for permutation in itertools.permutations(rows):
        if feed == "cocoa":
            result = list(module.CheckDuplicates().process(("2026-10-20", permutation)))
        elif feed == "oil":
            result = list(module.CheckUniqueness().process(("2026-10-20", permutation)))
        else:
            result = list(module.filter_unique_dates(("2026-10-20", permutation)))
        outputs.append(result)
    assert all(result == outputs[0] for result in outputs)
    assert rows == original
    valid = [r for r in outputs[0] if isinstance(r, dict)]
    invalid = [r for r in outputs[0] if isinstance(r, tuple) and r[0] == "invalid"]
    assert len(valid) == (0 if conflict else 1) and len(invalid) == (
        3 if conflict else 2
    )
