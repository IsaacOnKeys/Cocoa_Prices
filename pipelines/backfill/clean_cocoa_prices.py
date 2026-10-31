"""Isolated historical backfill for ICCO publisher-dated EUR/tonne CSV.

Requires an explicit held input generation/hash, date interval and candidate
dataset. See README.md and scripts/operations/backfill_history.py create.
No bare invocation or production WRITE_TRUNCATE destination is supported.
"""

import csv
import logging
import json
import math
import os
import time
from datetime import datetime

import apache_beam as beam
from backfill_recovery import configure_worker_package, invalid_record, resolve_duplicates
from backfill_contract import configure, finish, in_interval, persist_invalid
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    StandardOptions,
)

##################
# Configuration #
################

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

PIPELINE_OPTIONS = PipelineOptions()
STANDARD_OPTIONS = PIPELINE_OPTIONS.view_as(StandardOptions)
GCP_OPTIONS = PIPELINE_OPTIONS.view_as(GoogleCloudOptions)
RUNNER = STANDARD_OPTIONS.runner

if RUNNER == "DirectRunner":

    PROJECT_ID = os.getenv("PROJECT_ID", "cocoa-prices-430315")
    REGION = os.getenv("REGION", "europe-west3")
    STAGING_LOCATION = os.getenv("STAGING_LOCATION", "gs://raw-historic-data/staging")
    TEMP_LOCATION = os.getenv("TEMP_LOCATION", "gs://raw-historic-data/temp")
    WORKER_MACHINE_TYPE = os.getenv("WORKER_MACHINE_TYPE", "e2-standard-4")
    NUM_WORKERS = int(os.getenv("NUM_WORKERS", 4))
    MAX_NUM_WORKERS = int(os.getenv("MAX_NUM_WORKERS", NUM_WORKERS))
    REQUIREMENTS_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "requirements.txt"))
    GCP_OPTIONS.project = PROJECT_ID
    GCP_OPTIONS.region = REGION
    GCP_OPTIONS.staging_location = STAGING_LOCATION
    GCP_OPTIONS.temp_location = TEMP_LOCATION
    GCP_OPTIONS.job_name = f"clean-weather-data-{int(time.time()) % 100000}"


###############
# Transforms #
#############


def parse_csv(line):
    """
    Parses a CSV line into a dictionary with selected fields.

    Args:
        line (str): A single line of CSV data.

    Returns:
        dict: A dictionary containing:
            - "Date" (str): The date field from the CSV, with leading/trailing quotes removed.
            - "Euro_Price" (str): The price field from the CSV, with leading/trailing quotes removed.
    """
    try:
        reader = csv.reader([line])
        row = next(reader)
        return {
            "Date": row[0].strip('"'),
            "Euro_Price": row[4].strip('"'),
        }
    except Exception:
        return {"Date": None, "Euro_Price": None, "_parse_error": "malformed_csv", "_raw_line": line}


class ValidateAndTransform(beam.DoFn):
    """
    A DoFn class for validating and transforming parsed records.

    Validation:
        - Ensures the "Date" field is in the format "dd/mm/yyyy" and converts it to "yyyy-mm-dd".
        - Ensures the "Euro_Price" field is a positive float. If invalid, stores errors in an "Errors" field.

    Yields:
        - Valid records as-is.
        - Invalid records tagged with "invalid".

    Example:
        Input: {"Date": "31/12/2023", "Euro_Price": "1,234.56"}
        Output:
            - Valid: {"Date": "2023-12-31", "Euro_Price": 1234.56}
            - Invalid: {"Date": "invalid", "Euro_Price": "", "Errors": "Invalid date format"}
    """

    def process(self, element):
        if element.get('_parse_error'):
            yield beam.pvalue.TaggedOutput('invalid', element)
            return
        original = dict(element)
        valid = True
        errors = []

        # Validate and transform 'Date'
        try:
            date_obj = datetime.strptime(element["Date"], "%d/%m/%Y")
            element["Date"] = date_obj.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            valid = False
            errors.append(f"Invalid date format: {element['Date']}")

        # Validate and transform 'Euro_Price'
        euro_price_str = str(element.get("Euro_Price", "")).replace(",", "")
        try:
            element["Euro_Price"] = float(euro_price_str)
            if not math.isfinite(element["Euro_Price"]) or element["Euro_Price"] <= 0:
                valid = False
                errors.append("Euro_Price must be positive")
        except (ValueError, TypeError):
            valid = False
            errors.append("Invalid Euro_Price")

        if valid:
            yield element
        else:
            # Convert 'Euro_Price' to string for invalid records
            element["Euro_Price"] = str(element.get("Euro_Price", ""))
            element["Errors"] = "; ".join(errors)
            element['_raw_line'] = json.dumps(original, default=str)
            yield beam.pvalue.TaggedOutput("invalid", element)


class CheckDuplicates(beam.DoFn):
    """Emit one identical fact; reject all conflicting facts without an order winner."""

    def process(self, element):
        _, records = element
        unique, invalid = resolve_duplicates(records, 'cocoa')
        yield from unique
        for row in invalid:
            yield beam.pvalue.TaggedOutput('invalid', row)



#############
# Pipeline #
###########


def run():
    request, options = configure('cocoa')
    configure_worker_package(options, __file__)
    logging.info("Pipeline is starting...")
    with beam.Pipeline(options=options) as p:
        records = (
            p
            | "Read CSV"
            >> beam.io.ReadFromText(
                request.input_uri, skip_header_lines=1
            )
            | "Parse CSV" >> beam.Map(parse_csv)
        )

        validated_records = records | "Validate and Transform" >> beam.ParDo(
            ValidateAndTransform()
        ).with_outputs("invalid", main="valid")

        valid_records = validated_records.valid | "Select Requested Interval" >> beam.Filter(in_interval, request)
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
            table=request.table(),
            schema="Date:DATE, Euro_Price:FLOAT",
            write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location=f"gs://cocoa-prices-temp-for-bq/backfill/{request.token}/cocoa/",
        )

        evidence = all_invalid_records | "Retain Invalid Evidence" >> beam.Map(invalid_record, 'cocoa', request)
        (evidence | "Write Invalid to BigQuery" >> beam.io.WriteToBigQuery(
            table=request.table(invalid=True),
            schema="Date:STRING, Euro_Price:STRING, Errors:STRING",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location=f"gs://cocoa-prices-temp-for-bq/backfill/{request.token}/cocoa/",
        ))
        evidence | "Collect Rejections" >> beam.combiners.ToList() | "Archive Rejections" >> beam.Map(persist_invalid, request)
    finish(request, p.result)


if __name__ == "__main__":
    run()
