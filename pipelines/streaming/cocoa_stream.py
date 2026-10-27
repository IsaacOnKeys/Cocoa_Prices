import io
import json
import os

import apache_beam as beam
import fastavro
from apache_beam.options.pipeline_options import PipelineOptions

# ---- Config (env-first with safe defaults)
PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get(
    "PROJECT", "cocoa-prices-430315"
)

SUBSCRIPTION = os.environ.get(
    "COCOA_SUBSCRIPTION",
    os.environ.get(
        "PUBSUB_SUBSCRIPTION_COCOA",
        f"projects/{PROJECT}/subscriptions/cocoa-prices-sub",
    ),
)

BQ_TABLE = os.environ.get(
    "COCOA_BQ_TABLE",
    os.environ.get("BQ_TABLE_COCOA", f"{PROJECT}:stream_staging.cocoa_prices"),
)

AVRO_SCHEMA_PATH = os.environ.get(
    "COCOA_AVRO_SCHEMA_PATH",
    "/opt/beam/cocoa/schemas/cocoa_schema.avsc",
)
if not os.path.exists(AVRO_SCHEMA_PATH):
    AVRO_SCHEMA_PATH = "./schemas/cocoa_schema.avsc"

# ---- Timestamp normalization (matches the deployed cocoa consumer)
def _to_ts(v):
    from datetime import datetime, timezone

    def fmt(dt):
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")

    if isinstance(v, (int, float)):
        if v > 1e12:
            v /= 1000.0
        return fmt(datetime.fromtimestamp(v, tz=timezone.utc))
    if isinstance(v, str):
        try:
            return fmt(datetime.fromisoformat(v.replace("Z", "+00:00")))
        except Exception:
            pass
    return fmt(datetime.now(timezone.utc))


# ---- DoFns
class DecodeAvro(beam.DoFn):
    def __init__(self, schema_path):
        with open(schema_path, "r", encoding="utf-8") as f:
            self.schema = fastavro.parse_schema(json.load(f))

    def process(self, element):
        buf = io.BytesIO(element)
        try:
            yield fastavro.schemaless_reader(buf, self.schema)
        except Exception as e:
            print("Avro decode error:", e)


class ToBQRow(beam.DoFn):
    def process(self, record):
        yield {
            "date": record.get("date"),
            "cocoa_price": record.get("cocoa_price"),
            "ingestion_time": _to_ts(record.get("ingestion_time")),
            "raw_payload": record.get("raw_payload")
            or json.dumps(record, ensure_ascii=False),
        }


# ---- Pipeline
def run():
    options = PipelineOptions(
        ["--runner=DirectRunner", "--streaming", f"--project={PROJECT}"]
    )
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Decode Avro" >> beam.ParDo(DecodeAvro(AVRO_SCHEMA_PATH))
            | "ToBQRow" >> beam.ParDo(ToBQRow())
            | "WriteToBigQuery"
            >> beam.io.WriteToBigQuery(
                table=BQ_TABLE,
                schema="date:DATE,cocoa_price:FLOAT64,ingestion_time:TIMESTAMP,raw_payload:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )


if __name__ == "__main__":
    run()
