import io
import json

import apache_beam as beam
import fastavro
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT = "cocoa-prices-430315"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/oil-prices-sub"
BQ_TABLE = "cocoa-prices-430315:cocoa_related.brent_prices_temp"
AVRO_SCHEMA_PATH = "./schemas/oil_schema.avsc"


class DecodeAvro(beam.DoFn):
    def __init__(self, schema_path):
        with open(schema_path, "r") as f:
            self.schema = fastavro.parse_schema(json.load(f))

    def process(self, element):
        buf = io.BytesIO(element)
        try:
            record = fastavro.schemaless_reader(buf, self.schema)
            yield record
        except Exception as e:
            print("Avro decode error:", e)


class ToBQRow(beam.DoFn):
    """Map the Avro record to the BQ temp table schema."""

    def process(self, rec):
        def _to_float(v):
            try:
                return float(v) if v not in (None, "", ".") else None
            except Exception:
                return None

        yield {
            "date": rec.get("date"),  # 'YYYY-MM-DD' is fine for DATE
            "brent_price_eu": _to_float(
                rec.get("oil_price") or rec.get("brent_price_eu")
            ),
            "raw_payload": rec.get("raw_payload"),
            "ingestion_time": rec.get("ingestion_time"),
        }


def run():
    options = PipelineOptions(
        [
            "--runner=DirectRunner",
            "--streaming",
            f"--project={PROJECT}",
        ]
    )

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from Oil PubSub"
            >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Decode Avro" >> beam.ParDo(DecodeAvro(AVRO_SCHEMA_PATH))
            | "ToBQRow" >> beam.ParDo(ToBQRow())
            | "WriteToBigQuery"
            >> beam.io.WriteToBigQuery(
                table=BQ_TABLE,
                schema="date:DATE,brent_price_eu:FLOAT,raw_payload:STRING,ingestion_time:TIMESTAMP",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )


if __name__ == "__main__":
    run()
