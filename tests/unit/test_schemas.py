"""Round-trip the canonical wire contracts, including nullable fields."""

import io
import json
from pathlib import Path

import fastavro
import pytest

SCHEMAS = Path(__file__).resolve().parents[2] / "schemas"


@pytest.mark.parametrize("feed", ["cocoa", "oil", "weather"])
@pytest.mark.parametrize("nullable", [False, True])
def test_synthetic_binary_round_trip(feed, nullable):
    schema = fastavro.parse_schema(json.loads((SCHEMAS / f"{feed}_schema.avsc").read_bytes()))
    values = {"date": "2026-10-26", "ingestion_time": "2026-10-26T09:00:00Z", "raw_payload": "{}"}
    record = {
        field["name"]: None if nullable else values.get(field["name"], 1.25)
        for field in schema["fields"]
    }
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, schema, record)
    buffer.seek(0)
    assert fastavro.schemaless_reader(buffer, schema) == record


@pytest.mark.parametrize("feed", ["cocoa", "oil", "weather"])
def test_incompatible_measurement_type_is_rejected(feed):
    schema = fastavro.parse_schema(json.loads((SCHEMAS / f"{feed}_schema.avsc").read_bytes()))
    record = {field["name"]: None for field in schema["fields"]}
    measurement = next(field["name"] for field in schema["fields"] if "double" in field["type"])
    record[measurement] = {"unexpected": "object"}
    with pytest.raises((TypeError, ValueError)):
        fastavro.schemaless_writer(io.BytesIO(), schema, record)


@pytest.mark.parametrize("feed", ["cocoa", "oil", "weather"])
def test_truncated_binary_is_not_a_missing_observation(feed):
    schema = fastavro.parse_schema(json.loads((SCHEMAS / f"{feed}_schema.avsc").read_bytes()))
    with pytest.raises((EOFError, ValueError, IndexError)):
        fastavro.schemaless_reader(io.BytesIO(b""), schema)
