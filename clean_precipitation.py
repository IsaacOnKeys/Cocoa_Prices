import logging
import time

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
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

# Configure logging if needed
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def run():
    pipeline_options = PipelineOptions()

    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.requirements_file = "requirements.txt"
    setup_options.setup_file = './setup.py'

    standard_options = pipeline_options.view_as(StandardOptions)
    # standard_options.runner = "DataflowRunner"
    standard_options.runner = "DirectRunner"

    gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    gcp_options.project = "cocoa-prices-430315"
    # Ensure unique job name
    gcp_options.job_name = f"cleaning-weather-data-{int(time.time())}"
    gcp_options.staging_location = "gs://raw_historic_data/staging"
    gcp_options.temp_location = "gs://raw_historic_data/temp"
    gcp_options.region = "europe-west3"

    with beam.Pipeline(options=pipeline_options) as p:
        parsed_records = (
            p
            | "Read CSV"
            >> beam.io.ReadFromText(
                "gs://raw_historic_data/POWER_Point_Daily.csv",
                skip_header_lines=1,
            )
            | "Parse CSV" >> beam.Map(parse_csv)
            | "Filter Parsed Data" >> beam.Filter(lambda x: x is not None)
            | "Clean and Transform" >> beam.Map(clean_and_transform)
            | "Filter Missing Data" >> beam.Filter(filter_missing_data)
        )

        # Extract and Clean
        validated_records = (
            parsed_records
            | "Extract and Clean" >> beam.FlatMap(lambda x: [x])
            | "Validate and Transform"
            >> beam.ParDo(ValidateAndTransform()).with_outputs("invalid", main="valid")
        )

        # Handle Uniqueness Checks
        unique_records = (
            validated_records.valid
            | "Key by Date" >> beam.Map(key_by_date)
            | "Group by Date" >> beam.GroupByKey()
            | "Filter Unique Dates"
            >> beam.ParDo(filter_unique_dates).with_outputs(
                "invalid", main="valid_unique"
            )
        )

        # Integrate check_valid_record before writing to BigQuery
        unique_records_valid_checked = (
            unique_records.valid_unique
            | "Check Valid Records" >> beam.Map(check_valid_record)
        )

        # Write valid records to BigQuery
        (
            unique_records_valid_checked
            | "Write Valid to BigQuery"
            >> beam.io.WriteToBigQuery(
                table="cocoa-prices-430315:cocoa_prices.precipitation",
                schema="date:DATE, precipitation:FLOAT, soil_moisture:FLOAT",
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # Combine all invalid records from validation and uniqueness checks
        invalid_records = [
            validated_records.invalid,
            unique_records.invalid,
        ] | "Combine Invalid Records" >> beam.Flatten()

        # Write invalid records to BigQuery
        (
            invalid_records
            | "Write Invalid to BigQuery"
            >> beam.io.WriteToBigQuery(
                table="cocoa-prices-430315:cocoa_prices.invalid_precipitation",
                schema="date:STRING, precipitation:STRING, soil_moisture:STRING, Errors:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()
