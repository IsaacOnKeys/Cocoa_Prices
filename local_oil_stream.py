import io
import json

import apache_beam as beam
import fastavro
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT = "cocoa-prices-430315"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/oil-prices-sub"
BQ_TABLE = "cocoa-prices-430315:cocoa_related.brent_prices_temp"  # Updated temp target
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
    def process(self, record):
        print("Processing OIL record for BigQuery:", record)
        yield {
            "date": record.get("date"),
            "brent_price_eu": record.get("oil_price"),  # Match temp schema
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
                BQ_TABLE,
                schema="date:DATE,brent_price_eu:FLOAT64",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )


if __name__ == "__main__":
    run()
