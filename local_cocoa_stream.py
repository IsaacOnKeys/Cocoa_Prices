import io
import json

import apache_beam as beam
import fastavro
from apache_beam.options.pipeline_options import PipelineOptions

# ---- Configurations ----
PROJECT = "cocoa-prices-430315"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/cocoa-prices-sub"
BQ_TABLE = "cocoa-prices-430315:stream_staging.cocoa_prices"
AVRO_SCHEMA_PATH = "./schemas/cocoa_schema.avsc"


# ---- Avro Decode DoFn ----
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
            pass


# ---- BQ TableRow DoFn ----
class ToBQRow(beam.DoFn):
    def process(self, record):
        print("Processing record for BigQuery:", record)
        yield {
            "date": record.get("date"),
            "cocoa_price": record.get("cocoa_price"),
            "ingestion_time": record.get("ingestion_time"),
            "raw_payload": record.get("raw_payload"),
        }


# ---- Pipeline ----
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
            | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Decode Avro" >> beam.ParDo(DecodeAvro(AVRO_SCHEMA_PATH))
            | "ToBQRow" >> beam.ParDo(ToBQRow())
            | "WriteToBigQuery"
            >> beam.io.WriteToBigQuery(
                BQ_TABLE,
                schema="date:DATE,cocoa_price:FLOAT,ingestion_time:TIMESTAMP,raw_payload:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                row_id_fn=lambda row: row["date"],
            )
        )


if __name__ == "__main__":
    run()
