import io
import json
import os
from datetime import date, datetime, timedelta, timezone

import apache_beam as beam
import fastavro

# ---- Config (env-first with safe defaults)
PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get(
    "PROJECT", "cocoa-prices-430315"
)

SUBSCRIPTION = os.environ.get(
    "WEATHER_SUB",
    os.environ.get(
        "WEATHER_SUBSCRIPTION",
        os.environ.get(
            "PUBSUB_SUBSCRIPTION_WEATHER",
            f"projects/{PROJECT}/subscriptions/weather-data-sub",
        ),
    ),
)

BQ_TABLE = os.environ.get(
    "WEATHER_TABLE",
    os.environ.get(
        "WEATHER_BQ_TABLE",
        os.environ.get(
            "BQ_TABLE_WEATHER", f"{PROJECT}:stream_staging.precipitation_moisture"
        ),
    ),
)

AVRO_SCHEMA_PATH = os.environ.get(
    "WEATHER_AVRO_SCHEMA_PATH",
    "/opt/beam/weather/schemas/weather_schema.avsc",
)
if not os.path.exists(AVRO_SCHEMA_PATH):
    AVRO_SCHEMA_PATH = "./schemas/weather_schema.avsc"

# ---- Helpers (shared)
_EPOCH = date(1970, 1, 1)


def _to_float(v):
    try:
        return float(v) if v not in (None, "", ".") else None
    except Exception:
        return None


def _avro_date_to_str(v):
    if isinstance(v, int):
        try:
            return (_EPOCH + timedelta(days=v)).isoformat()
        except Exception:
            return None
    if isinstance(v, str):
        return v[:10]
    return None


def _now_utc_rfc3339():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ---- DoFns
class DecodeAvro(beam.DoFn):
    def __init__(self, schema_path):
        with open(schema_path, "r", encoding="utf-8") as f:
            self.schema = fastavro.parse_schema(json.load(f))

    def process(self, element):
        buf = io.BytesIO(element)
        try:
            record = fastavro.schemaless_reader(buf, self.schema)
            if buf.read(1):
                raise ValueError("Trailing Avro data")
        except Exception:
            raise RuntimeError("Legacy Avro decode failed; recover the original input") from None
        yield record


class ToBQRow(beam.DoFn):
    def process(self, record):
        yield {
            "date": _avro_date_to_str(record.get("date")),
            "precipitation": _to_float(record.get("precipitation")),
            "soil_moisture": _to_float(record.get("soil_moisture")),
            "ingestion_time": _now_utc_rfc3339(),
            "raw_payload": record.get("raw_payload")
            or json.dumps(record, ensure_ascii=False),
        }


# ---- Pipeline
def run():
    raise RuntimeError(
        "Legacy daily Beam streaming is retired. Use the Cloud Run consumer and "
        "scripts/operations/replay_daily_inputs.py for retained-input recovery. "
        "Apache Beam remains supported in pipelines/backfill."
    )


if __name__ == "__main__":
    run()
