"""Exercise real Avro/row transforms with fake Beam IO and no cloud access."""

import importlib.util
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import fastavro
import pytest

ROOT = Path(__file__).resolve().parents[2]
FEEDS = ("cocoa", "oil", "weather")
MEASUREMENTS = {
    "cocoa": {"cocoa_price": 6500.25},
    "oil": {"oil_price": 72.5},
    "weather": {"precipitation": 3.5, "soil_moisture": 0.42},
}
SOURCE_TIME = "2026-10-20T10:15:30Z"
RECEIPT_TIME = "2026-10-26T12:00:00Z"
ENVIRONMENT_KEYS = (
    "GOOGLE_CLOUD_PROJECT", "PROJECT", "COCOA_SUBSCRIPTION",
    "PUBSUB_SUBSCRIPTION_COCOA", "COCOA_BQ_TABLE", "BQ_TABLE_COCOA",
    "COCOA_AVRO_SCHEMA_PATH", "OIL_SUBSCRIPTION", "PUBSUB_SUBSCRIPTION_OIL",
    "OIL_BQ_TABLE", "BQ_TABLE_OIL", "OIL_AVRO_SCHEMA_PATH", "WEATHER_SUB",
    "WEATHER_SUBSCRIPTION", "PUBSUB_SUBSCRIPTION_WEATHER", "WEATHER_TABLE",
    "WEATHER_BQ_TABLE", "BQ_TABLE_WEATHER", "WEATHER_AVRO_SCHEMA_PATH",
)


def load_consumer(monkeypatch, feed, *, source=None, env=None):
    """Load an actual consumer module; only its external Beam boundary is fake."""
    for key in ENVIRONMENT_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in (env or {}).items():
        monkeypatch.setenv(key, value)
    monkeypatch.chdir(ROOT)
    beam = ModuleType("apache_beam")
    beam.__path__ = []
    beam.DoFn = object
    beam.Pipeline = MagicMock()
    beam.ParDo = MagicMock()
    beam.io = SimpleNamespace(
        ReadFromPubSub=MagicMock(),
        WriteToBigQuery=MagicMock(),
        BigQueryDisposition=SimpleNamespace(WRITE_APPEND="WRITE_APPEND", CREATE_NEVER="CREATE_NEVER"),
    )
    options = ModuleType("apache_beam.options")
    options.__path__ = []
    pipeline_options = ModuleType("apache_beam.options.pipeline_options")
    pipeline_options.PipelineOptions = MagicMock()
    for module in (beam, options, pipeline_options):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    path = source or ROOT / "pipelines/streaming" / f"{feed}_stream.py"
    spec = importlib.util.spec_from_file_location(f"{feed}_consumer_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if hasattr(module, "_now_utc_rfc3339"):
        monkeypatch.setattr(module, "_now_utc_rfc3339", lambda: RECEIPT_TIME)
    return SimpleNamespace(module=module, beam=beam, options=pipeline_options)


def encode_record(feed, record):
    schema = fastavro.parse_schema(json.loads((ROOT / "schemas" / f"{feed}_schema.avsc").read_bytes()))
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, schema, record)
    return buffer.getvalue()


@pytest.mark.parametrize("feed", FEEDS)
def test_wire_record_maps_to_existing_staging_contract(feed, monkeypatch):
    consumer = load_consumer(monkeypatch, feed)
    record = {
        "date": "2026-10-20", **MEASUREMENTS[feed],
        "ingestion_time": SOURCE_TIME, "raw_payload": '{"source":"fixture"}',
    }
    decoder = consumer.module.DecodeAvro(ROOT / "schemas" / f"{feed}_schema.avsc")
    decoded = list(decoder.process(encode_record(feed, record)))
    assert decoded == [record]
    rows = list(consumer.module.ToBQRow().process(decoded[0]))
    expected_time = "2026-10-20 10:15:30.000000 UTC" if feed == "cocoa" else RECEIPT_TIME
    assert rows == [{**record, "ingestion_time": expected_time}]
    consumer.beam.Pipeline.assert_not_called()
    consumer.beam.io.WriteToBigQuery.assert_not_called()


@pytest.mark.parametrize("feed", FEEDS)
def test_nullable_measurements_and_raw_payload_fallback(feed, monkeypatch):
    consumer = load_consumer(monkeypatch, feed)
    record = {
        "date": "2026-10-20", **dict.fromkeys(MEASUREMENTS[feed]),
        "ingestion_time": SOURCE_TIME, "raw_payload": None,
    }
    decoder = consumer.module.DecodeAvro(ROOT / "schemas" / f"{feed}_schema.avsc")
    decoded = list(decoder.process(encode_record(feed, record)))[0]
    row = list(consumer.module.ToBQRow().process(decoded))[0]
    assert row["date"] == record["date"]
    assert all(row[name] is None for name in MEASUREMENTS[feed])
    assert json.loads(row["raw_payload"]) == record


@pytest.mark.parametrize("feed", FEEDS)
def test_invalid_avro_is_not_emitted_as_a_row(feed, monkeypatch, capsys):
    consumer = load_consumer(monkeypatch, feed)
    decoder = consumer.module.DecodeAvro(ROOT / "schemas" / f"{feed}_schema.avsc")
    assert list(decoder.process(b"")) == []
    assert "Avro decode error:" in capsys.readouterr().out


@pytest.mark.parametrize(("value", "expected"), [
    ("2026-10-20T10:15:30Z", "2026-10-20 10:15:30.000000 UTC"),
    ("2026-10-20T12:15:30+02:00", "2026-10-20 10:15:30.000000 UTC"),
    (1700000000, "2023-11-14 22:13:20.000000 UTC"),
    (1700000000000, "2023-11-14 22:13:20.000000 UTC"),
])
def test_cocoa_preserves_deployed_timestamp_normalization(value, expected, monkeypatch):
    consumer = load_consumer(monkeypatch, "cocoa")
    assert consumer.module._to_ts(value) == expected


@pytest.mark.parametrize("value", [None, "invalid timestamp"])
def test_cocoa_timestamp_fallback_is_current_utc(value, monkeypatch):
    consumer = load_consumer(monkeypatch, "cocoa")
    before = datetime.now(timezone.utc)
    result = consumer.module._to_ts(value)
    after = datetime.now(timezone.utc)
    parsed = datetime.strptime(result, "%Y-%m-%d %H:%M:%S.%f UTC").replace(tzinfo=timezone.utc)
    assert before <= parsed <= after


@pytest.mark.parametrize(("key", "attribute"), [
    ("WEATHER_SUB", "SUBSCRIPTION"),
    ("WEATHER_SUBSCRIPTION", "SUBSCRIPTION"),
    ("PUBSUB_SUBSCRIPTION_WEATHER", "SUBSCRIPTION"),
    ("WEATHER_TABLE", "BQ_TABLE"),
    ("WEATHER_BQ_TABLE", "BQ_TABLE"),
    ("BQ_TABLE_WEATHER", "BQ_TABLE"),
])
def test_weather_supports_deployed_and_repository_aliases(key, attribute, monkeypatch):
    expected = "projects/fixture/subscriptions/weather" if attribute == "SUBSCRIPTION" else "fixture:staging.weather"
    consumer = load_consumer(monkeypatch, "weather", env={key: expected})
    assert getattr(consumer.module, attribute) == expected


def test_deployed_weather_aliases_win_when_both_are_present(monkeypatch):
    consumer = load_consumer(monkeypatch, "weather", env={
        "WEATHER_SUB": "projects/fixture/subscriptions/live",
        "WEATHER_SUBSCRIPTION": "projects/fixture/subscriptions/repository",
        "WEATHER_TABLE": "fixture:staging.live",
        "WEATHER_BQ_TABLE": "fixture:staging.repository",
    })
    assert consumer.module.SUBSCRIPTION == "projects/fixture/subscriptions/live"
    assert consumer.module.BQ_TABLE == "fixture:staging.live"


@pytest.mark.parametrize("feed", FEEDS)
def test_schema_override_remains_supported(feed, monkeypatch):
    path = str(ROOT / "schemas" / f"{feed}_schema.avsc")
    consumer = load_consumer(monkeypatch, feed, env={feed.upper() + "_AVRO_SCHEMA_PATH": path})
    assert consumer.module.AVRO_SCHEMA_PATH == path


@pytest.mark.parametrize(("key", "attribute", "expected"), [
    ("COCOA_SUBSCRIPTION", "SUBSCRIPTION", "projects/fixture/subscriptions/cocoa"),
    ("PUBSUB_SUBSCRIPTION_COCOA", "SUBSCRIPTION", "projects/fixture/subscriptions/cocoa"),
    ("COCOA_BQ_TABLE", "BQ_TABLE", "fixture:staging.cocoa"),
    ("BQ_TABLE_COCOA", "BQ_TABLE", "fixture:staging.cocoa"),
    ("PROJECT", "PROJECT", "fixture-project"),
    ("GOOGLE_CLOUD_PROJECT", "PROJECT", "fixture-project"),
])
def test_cocoa_repository_configuration_is_retained(key, attribute, expected, monkeypatch):
    consumer = load_consumer(monkeypatch, "cocoa", env={key: expected})
    assert getattr(consumer.module, attribute) == expected


@pytest.mark.parametrize(("feed", "subscription", "table"), [
    ("cocoa", "cocoa-prices-sub", "stream_staging.cocoa_prices"),
    ("oil", "oil-prices-sub", "stream_staging.oil_prices"),
    ("weather", "weather-data-sub", "stream_staging.precipitation_moisture"),
])
def test_pipeline_construction_keeps_runner_and_destinations(feed, subscription, table, monkeypatch):
    consumer = load_consumer(monkeypatch, feed)
    consumer.module.run()
    consumer.options.PipelineOptions.assert_called_once_with(
        ["--runner=DirectRunner", "--streaming", "--project=cocoa-prices-430315"]
    )
    consumer.beam.io.ReadFromPubSub.assert_called_once_with(
        subscription=f"projects/cocoa-prices-430315/subscriptions/{subscription}"
    )
    call = consumer.beam.io.WriteToBigQuery.call_args
    assert call.kwargs["table"] == f"cocoa-prices-430315:{table}"
    assert call.kwargs["write_disposition"] == "WRITE_APPEND"
    assert call.kwargs["create_disposition"] == "CREATE_NEVER"
