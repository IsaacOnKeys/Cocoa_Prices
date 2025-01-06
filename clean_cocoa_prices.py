import logging
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)

from src.cocoa_package import CheckDuplicates, ValidateAndTransform, parse_csv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

PROJECT_ID = "cocoa-prices-430315"
STAGING_LOCATION = "gs://raw_historic_data/staging"
TEMP_LOCATION = "gs://raw_historic_data/temp"
SOURCE_FILE = "gs://raw_historic_data/Daily Prices_Home.csv"
VALID_TABLE = f"{PROJECT_ID}:cocoa_prices.cocoa"
INVALID_TABLE = f"{PROJECT_ID}:cocoa_prices.invalid_cocoa"
pipeline_options = PipelineOptions()

setup_options = pipeline_options.view_as(SetupOptions)
setup_options.save_main_session = True
setup_options.requirements_file = "requirements.txt"
standard_options = pipeline_options.view_as(StandardOptions)
# standard_options.runner = "DataflowRunner"
standard_options.runner = "DirectRunner"

gcp_options = pipeline_options.view_as(GoogleCloudOptions)
gcp_options.project = PROJECT_ID
gcp_options.job_name = f"cleaning-cocoa-prices-data-{int(time.time())}"
gcp_options.staging_location = STAGING_LOCATION
gcp_options.temp_location = TEMP_LOCATION
gcp_options.region = "europe-west3"

def run():
    logging.info("Pipeline is starting...")
    with beam.Pipeline(options=pipeline_options) as p:
        records = (
            p
            | "Read CSV" >> beam.io.ReadFromText(SOURCE_FILE, skip_header_lines=1)
            | "Parse CSV" >> beam.Map(parse_csv)
        )

        validated_records = records | "Validate and Transform" >> beam.ParDo(
            ValidateAndTransform()
        ).with_outputs("invalid", main="valid")

        valid_records = validated_records.valid
        invalid_records = validated_records.invalid

        duplicate_checked = (
            valid_records
            | "Key by Date" >> beam.Map(lambda x: (x["Date"], x))
            | "Group by Date" >> beam.GroupByKey()
            | "Check Duplicates"
            >> beam.ParDo(CheckDuplicates()).with_outputs("invalid", main="unique")
        )

        unique_records = duplicate_checked.unique
        duplicate_records = duplicate_checked.invalid

        all_invalid_records = [
            invalid_records,
            duplicate_records,
        ] | "Combine Invalid Records" >> beam.Flatten()

        unique_records | "Write Valid to BigQuery" >> beam.io.WriteToBigQuery(
            table=VALID_TABLE,
            schema="Date:DATE, Euro_Price:FLOAT",
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

        all_invalid_records | "Write Invalid to BigQuery" >> beam.io.WriteToBigQuery(
            table=INVALID_TABLE,
            schema="Date:STRING, Euro_Price:STRING, Errors:STRING",
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )
    logging.info("Pipeline run has completed")

if __name__ == "__main__":
    run()
