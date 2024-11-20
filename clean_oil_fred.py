import json
import logging
import time

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)

from src.oil_package import CheckUniqueness, ValidateAndTransform, extract_and_clean

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def run():
    pipeline_options = PipelineOptions()

    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.requirements_file = "requirements.txt"
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.runner = "DirectRunner"  # Use 'DataflowRunner' for cloud execution
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    gcp_options.project = "cocoa-prices-430315"
    gcp_options.job_name = f"cleaning-oil-data-{int(time.time())}"
    gcp_options.staging_location = "gs://raw_historic_data/staging"
    gcp_options.temp_location = "gs://raw_historic_data/temp"
    gcp_options.region = "europe-west3"

    with beam.Pipeline(options=pipeline_options) as p:
        validated_records = (
            p
            | "Match Files"
            >> fileio.MatchFiles("gs://raw_historic_data/brent_oil_fred.json")
            | "Read Matches" >> fileio.ReadMatches()
            | "Read File Content" >> beam.Map(lambda file: file.read_utf8())
            | "Extract and Clean" >> beam.FlatMap(extract_and_clean)
            | "Validate and Transform"
            >> beam.ParDo(ValidateAndTransform()).with_outputs("invalid", main="valid")
        )

        valid_records = validated_records.valid
        invalid_records = validated_records.invalid

        grouped_by_date = (
            valid_records
            | "Group by Date" >> beam.Map(lambda x: (x["date"], x))
            | "Group ByKey" >> beam.GroupByKey()
        )

        final_output = grouped_by_date | "Check Uniqueness" >> beam.ParDo(
            CheckUniqueness()
        ).with_outputs("invalid", main="unique")

        unique_records = final_output.unique
        invalid_duplicates = final_output.invalid

        (
            unique_records
            | "Write Valid to BigQuery"
            >> beam.io.WriteToBigQuery(
                table="cocoa-prices-430315:cocoa_prices.brent_prices",
                schema="date:DATE, brent_price_eu:FLOAT",
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        combined_invalid_records = [
            invalid_records,
            invalid_duplicates,
        ] | "Combine Invalid Records" >> beam.Flatten()

        (
            combined_invalid_records
            | "Write Invalid to BigQuery"
            >> beam.io.WriteToBigQuery(
                table="cocoa-prices-430315:cocoa_prices.invalid_brent_prices",
                schema="date:STRING, brent_price_eu:STRING, Errors:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()
