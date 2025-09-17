import io
import json
from datetime import datetime, timezone

import apache_beam as beam
import fastavro
from apache_beam.options.pipeline_options import PipelineOptions

# ---- Configurations ----
PROJECT = "cocoa-prices-430315"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/cocoa-prices-sub"
BQ_TABLE = "cocoa-prices-430315:stream_staging.cocoa_prices"
AVRO_SCHEMA_PATH = "./schemas/cocoa_schema.avsc"


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


# ---- Avro Decode DoFn ----
class DecodeAvro(beam.DoFn):
    def __init__(self, schema_path):
        with open(schema_path, "r") as f:
            self.schema = fastavro.parse_schema(json.load(f))

    def process(self, element):
        buf = io.BytesIO(element)
        try:
            yield fastavro.schemaless_reader(buf, self.schema)
        except Exception as e:
            print("Avro decode error:", e)


# ---- BQ TableRow DoFn ----
class ToBQRow(beam.DoFn):
    def process(self, record):
        yield {
            "date": record.get("date"),
            "cocoa_price": record.get("cocoa_price"),
            "ingestion_time": _to_ts(record.get("ingestion_time")),
            "raw_payload": record.get("raw_payload")
            or json.dumps(record, ensure_ascii=False),
        }


# ---- Pipeline ----
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
                BQ_TABLE,
                schema="date:DATE,cocoa_price:FLOAT64,ingestion_time:TIMESTAMP,raw_payload:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )


if __name__ == "__main__":
    run()
