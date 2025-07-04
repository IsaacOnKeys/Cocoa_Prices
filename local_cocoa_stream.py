import io
import json

import apache_beam as beam
import fastavro
from apache_beam.options.pipeline_options import PipelineOptions

# ---- Configurations ----
PROJECT = "cocoa-prices-430315"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/cocoa-prices-sub"
BQ_TABLE = "cocoa-prices-430315:cocoa_related.cocoa_temp"  # Updated temp sink
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
            "Date": record.get("date"),               # Capitalized to match schema
            "Euro_Price": record.get("cocoa_price"),  # Match temp schema field
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
                schema="Date:DATE,Euro_Price:FLOAT64,ingestion_time:TIMESTAMP,raw_payload:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            )
        )


if __name__ == "__main__":
    run()