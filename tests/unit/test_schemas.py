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
