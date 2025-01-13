"""
Usage:
To run with DirectRunner (default):

python clean_precipitation.py

To run with DataFlow:

python clean_precipitation.py --runner=DataflowRunner --prebuild_sdk_container

"""

import logging
import os
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
    WorkerOptions,
)

from src.weather_package import (
    ValidateAndTransform,
    check_valid_record,
    clean_and_transform,
    filter_missing_data,
    filter_unique_dates,
    key_by_date,
    parse_csv,
)

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Dynamic Pipeline Options
PIPELINE_OPTIONS = PipelineOptions()
STANDARD_OPTIONS = PIPELINE_OPTIONS.view_as(StandardOptions)
GCP_OPTIONS = PIPELINE_OPTIONS.view_as(GoogleCloudOptions)
RUNNER = STANDARD_OPTIONS.runner

# Validate Runner
if RUNNER not in ["DirectRunner", "DataflowRunner"]:
    raise ValueError(f"Unsupported runner: {STANDARD_OPTIONS.runner}")

# Environment Variables
PROJECT_ID = os.getenv("PROJECT_ID", "cocoa-prices-430315")
REGION = os.getenv("REGION", "europe-west3")
STAGING_LOCATION = os.getenv("STAGING_LOCATION", "gs://raw-historic-data/staging")
TEMP_LOCATION = os.getenv("TEMP_LOCATION", "gs://raw-historic-data/temp")
WORKER_MACHINE_TYPE = os.getenv("WORKER_MACHINE_TYPE", "e2-standard-4")
NUM_WORKERS = int(os.getenv("NUM_WORKERS", 4))
MAX_NUM_WORKERS = int(os.getenv("MAX_NUM_WORKERS", NUM_WORKERS))
REQUIREMENTS_FILE = "./requirements.txt"
SETUP_FILE = "./setup.py"
# Google Cloud Configuration
GCP_OPTIONS.project = PROJECT_ID
GCP_OPTIONS.region = REGION
GCP_OPTIONS.staging_location = STAGING_LOCATION
GCP_OPTIONS.temp_location = TEMP_LOCATION
GCP_OPTIONS.job_name = f"clean-weather-data-{int(time.time()) % 100000}"

# Runner-specific Configuration
if RUNNER == "DataflowRunner":
    PIPELINE_OPTIONS.view_as(PipelineOptions).set_default(
    'extra_packages', ['dist/cocoa_code-0.1.tar.gz'])
    STANDARD_OPTIONS.prebuild_sdk_container = True
    WORKER_OPTIONS = PIPELINE_OPTIONS.view_as(WorkerOptions)
    WORKER_OPTIONS.num_workers = NUM_WORKERS
    WORKER_OPTIONS.max_num_workers = MAX_NUM_WORKERS
    WORKER_OPTIONS.machine_type = WORKER_MACHINE_TYPE

    SETUP_OPTIONS = PIPELINE_OPTIONS.view_as(SetupOptions)
    SETUP_OPTIONS.requirements_file = os.getenv("REQUIREMENTS_FILE", "./requirements.txt")
    SETUP_OPTIONS.setup_file = os.getenv("SETUP_FILE", "./setup.py")


def run():
    logging.info("Pipeline is starting...")
    logging.info(f"Runner = {RUNNER}")
    with beam.Pipeline(options=PIPELINE_OPTIONS) as p:
        parsed_records = (
            p
            | "Read CSV"
            >> beam.io.ReadFromText(
                "gs://raw-historic-data/POWER_Point_Daily.csv",
                skip_header_lines=1,
            )
            | "Parse CSV" >> beam.Map(parse_csv)
            | "Filter Parsed Data" >> beam.Filter(lambda x: x is not None)
            | "Filter Missing Data" >> beam.Filter(filter_missing_data)
            | "Clean and Transform" >> beam.Map(clean_and_transform)
        )

        validated_records = (
            parsed_records
            | "Extract and Clean" >> beam.FlatMap(lambda x: [x])
            | "Validate and Transform"
            >> beam.ParDo(ValidateAndTransform()).with_outputs("invalid", main="valid")
        )

        valid_records = validated_records.valid
        invalid_records = validated_records.invalid

        unique_records = (
            valid_records
            | "Key by Date" >> beam.Map(key_by_date)
            | "Debug Before Reshuffle"
            >> beam.Map(lambda x: logging.info(f"Before Reshuffle: {x}") or x)
            | "Explicit Reshuffle" >> beam.Reshuffle()
            | "Debug After Reshuffle"
            >> beam.Map(lambda x: logging.info(f"After Reshuffle: {x}") or x)
            | "Group by Date" >> beam.GroupByKey()
            | "Filter Unique Dates"
            >> beam.ParDo(filter_unique_dates).with_outputs(
                "invalid", main="valid_unique"
            )
        )

        unique_records_valid_checked = (
            unique_records.valid_unique
            | "Check Valid Records" >> beam.Map(check_valid_record)
        )

        (
            unique_records_valid_checked
            | "Write Valid to BigQuery"
            >> beam.io.WriteToBigQuery(
                table="cocoa-prices-430315:cocoa_related.precipitation",
                schema="date:DATE, precipitation:FLOAT, soil_moisture:FLOAT",
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location="gs://cocoa-prices-temp-for-bq",
            )
        )

        combined_invalid_records = [
            invalid_records,
            unique_records.invalid,
        ] | "Combine Invalid Records" >> beam.Flatten()

        (
            combined_invalid_records
            | "Write Invalid to BigQuery"
            >> beam.io.WriteToBigQuery(
                table="cocoa-prices-430315:cocoa_related.invalid_precipitation",
                schema="date:STRING, precipitation:STRING, soil_moisture:STRING, Errors:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location="gs://cocoa-prices-temp-for-bq",
            )
        )


if __name__ == "__main__":
    run()
