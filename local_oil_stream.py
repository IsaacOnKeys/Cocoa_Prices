import io
import json
from datetime import date as date_cls
from datetime import datetime, timezone

import apache_beam as beam
import fastavro
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT = "cocoa-prices-430315"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/oil-prices-sub"
BQ_TABLE = "cocoa-prices-430315:stream_staging.oil_prices"
AVRO_SCHEMA_PATH = "./schemas/oil_schema.avsc"


def _to_ts(v):
    if isinstance(v, (int, float)):
        if v > 1e12:  # millis -> secs
            v /= 1000.0
        return datetime.fromtimestamp(v, tz=timezone.utc)
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            pass
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def _to_date(v):
    if isinstance(v, (int, float)):
        if v > 1e12:
            v /= 1000.0
        return datetime.utcfromtimestamp(v).date().isoformat()
    if isinstance(v, datetime):
        return v.date().isoformat()
    if isinstance(v, date_cls):
        return v.isoformat()
    if isinstance(v, str):
        return v[:10]  # assume YYYY-MM-DD...
    return datetime.now(timezone.utc).date().isoformat()


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


class ToBQRow(beam.DoFn):
    def process(self, rec):
        def _to_float(v):
            try:
                return float(v) if v not in (None, "", ".") else None
            except Exception:
                return None

        yield {
            "date": _to_date(rec.get("date")),
            "oil_price": _to_float(rec.get("oil_price")),
            "ingestion_time": _to_ts(rec.get("ingestion_time")),
            "raw_payload": rec.get("raw_payload") or json.dumps(rec, ensure_ascii=False),
        }


def run():
    options = PipelineOptions(
        ["--runner=DirectRunner", "--streaming", f"--project={PROJECT}"]
    )
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Decode Avro" >> beam.ParDo(DecodeAvro(AVRO_SCHEMA_PATH))
            | "ToBQRow" >> beam.ParDo(ToBQRow())
            | "WriteToBigQuery"
            >> beam.io.WriteToBigQuery(
                table=BQ_TABLE,
                schema="date:DATE,oil_price:FLOAT64,ingestion_time:TIMESTAMP,raw_payload:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
            )
        )


if __name__ == "__main__":
    run()
