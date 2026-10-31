"""Isolated historical backfill for EIA published-table Brent USD/barrel JSON.

Requires an explicit held input generation/hash, date interval and candidate
dataset. See README.md and scripts/operations/backfill_history.py create.
No bare invocation or production WRITE_TRUNCATE destination is supported.
"""

import json
import logging
import math
import os
import time
from datetime import datetime, timezone

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


def extract_and_clean(file_content):
    """
    Processes the JSON object, extracts each observation, converts the value
    to a float if possible, and yields a dictionary with 'date' and 'brent_price_eu'.
    """
    if (file_content.get("series") != "RBRTE" or file_content.get("units") != "$/BBL"
            or file_content.get("acquisition_interface") != "published-table"):
        raise ValueError("Expected an accepted EIA published-table archive")
    observations = file_content["response"]["data"]
    for obs in observations:
        if obs.get("series") != "RBRTE" or obs.get("units") != "$/BBL":
            raise ValueError("Unexpected EIA archive series or units")
        date = obs.get("period")
        value = obs.get("value")
        try:
            value = float(value) if value not in (None, "", ".", "NA", "--", "-") else None
        except (ValueError, TypeError):
            value = None
        record = {"date": date, "brent_price_eu": value}
        if value is None or not math.isfinite(value):
            record['_raw_line'] = json.dumps(obs)
        yield record


class ValidateAndTransform(beam.DoFn):
    """
    Validates and transforms observations by checking date range,
    verifying numeric fields, and tagging invalid records.
    """

    def __init__(self):
        super(ValidateAndTransform, self).__init__()
        self.start_date = datetime(2014, 1, 1)
        self.end_date = datetime.now(timezone.utc).replace(tzinfo=None)

    def process(self, element):
        """
        Checks date formatting, range validity, and ensures brent_price_eu is float.
        Yields valid records or tags invalid ones with an error message.
        """
        valid = True
        errors = []
        result = {}
        if "date" not in element or "brent_price_eu" not in element:
            valid = False
            errors.append("Missing 'date' or 'brent_price_eu' field")
        try:
            date_obj = datetime.strptime(element["date"], "%Y-%m-%d")
            result["date"] = date_obj
            if not (self.start_date <= date_obj <= self.end_date):
                valid = False
                errors.append("Date out of range")
        except (ValueError, TypeError):
            valid = False
            errors.append("Invalid date format")
        brent_price = element.get("brent_price_eu")
        if brent_price is None:
            valid = False
            errors.append("Missing Brent_Price")
        elif not isinstance(brent_price, float):
            valid = False
            errors.append("Brent_Price is not a float")
        elif not math.isfinite(brent_price):
            valid = False
            errors.append("Brent_Price is not finite")
        if valid:
            yield {
                "date": result["date"].strftime("%Y-%m-%d"),
                "brent_price_eu": brent_price,
            }
        else:
            element["Errors"] = "; ".join(errors)
            yield beam.pvalue.TaggedOutput("invalid", element)


class CheckUniqueness(beam.DoFn):
    """
    Checks for duplicate records by grouping on the date key.
    Yields valid records or tags duplicates as invalid.
    """

    def process(self, element):
        _, records = element
        unique, invalid = resolve_duplicates(records, 'oil')
        yield from unique
        for row in invalid:
            yield beam.pvalue.TaggedOutput('invalid', row)



#############
# Pipeline #
###########


def run():
    request, options = configure('oil')
    configure_worker_package(options, __file__)
    logging.info("Pipeline is starting...")
    with beam.Pipeline(options=options) as p:
        validated_records = (
            p
            | "Read Lines"
            >> beam.io.ReadFromText(request.input_uri)
            | "Combine Lines"
            >> beam.CombineGlobally(lambda lines: "\n".join(lines)).without_defaults()
            | "Parse JSON" >> beam.Map(json.loads)
            | "Extract and Clean" >> beam.FlatMap(extract_and_clean)
            | "Validate and Transform"
            >> beam.ParDo(ValidateAndTransform()).with_outputs("invalid", main="valid")
        )

        valid_records = validated_records.valid | "Select Requested Interval" >> beam.Filter(in_interval, request)
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
                table=request.table(),
                schema="date:DATE, brent_price_eu:FLOAT",
                write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=f"gs://cocoa-prices-temp-for-bq/backfill/{request.token}/oil/",
            )
        )

        combined_invalid_records = [
            invalid_records,
            invalid_duplicates,
        ] | "Combine Invalid Records" >> beam.Flatten()

        evidence = combined_invalid_records | "Retain Invalid Evidence" >> beam.Map(invalid_record, 'oil', request)
        (
            evidence | "Write Invalid to BigQuery"
            >> beam.io.WriteToBigQuery(
                table=request.table(invalid=True),
                schema="date:STRING, brent_price_eu:STRING, Errors:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=f"gs://cocoa-prices-temp-for-bq/backfill/{request.token}/oil/",
            )
        )
        evidence | "Collect Rejections" >> beam.combiners.ToList() | "Archive Rejections" >> beam.Map(persist_invalid, request)
    finish(request, p.result)


if __name__ == "__main__":
    run()
