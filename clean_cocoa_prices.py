"""
Usage:

COCOA DATA MUST BE DOWNLOADED 
from icco.org/statistics
	filter for start and end dates
	set to "show all"
	download

To run with DataFlow:

python clean_precipitation.py \
--runner=DataflowRunner \
--job_name="clean-weather-data-$(date +%s)" \
--project=cocoa-prices-430315 \
--region=europe-west3 \
--temp_location=gs://raw-historic-data/temp \
--staging_location=gs://raw-historic-data/staging \
--worker_machine_type=e2-standard-4 \
--save_main_session \
--worker_harness_container_image=europe-west3-docker.pkg.dev/cocoa-prices-430315/cocoa-code-project/dataflow-pipelines-batch:latest

"""

import csv
import logging
import os
import time
from datetime import datetime

import apache_beam as beam
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
    REQUIREMENTS_FILE = "./requirements.txt"
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
    except Exception as e:
        logging.warning(f"Skipping malformed line: {line}, Error: {e}")
        return None


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
        valid = True
        errors = []

        # Validate and transform 'Date'
        try:
            date_obj = datetime.strptime(element["Date"], "%d/%m/%Y")
            element["Date"] = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            valid = False
            errors.append(f"Invalid date format: {element['Date']}")

        # Validate and transform 'Euro_Price'
        euro_price_str = element["Euro_Price"].replace(",", "")
        try:
            element["Euro_Price"] = float(euro_price_str)
            if element["Euro_Price"] <= 0:
                valid = False
                errors.append("Euro_Price must be positive")
        except ValueError:
            valid = False
            errors.append("Invalid Euro_Price")

        if valid:
            yield element
        else:
            # Convert 'Euro_Price' to string for invalid records
            element["Euro_Price"] = str(element.get("Euro_Price", ""))
            element["Errors"] = "; ".join(errors)
            yield beam.pvalue.TaggedOutput("invalid", element)


class CheckDuplicates(beam.DoFn):
    """
    A DoFn class for identifying and handling duplicate records.

    Process:
        - For a given key (e.g., "Date"), yields the first record as unique.
        - Tags subsequent records with "invalid" and annotates them with an error message.

    Args:
        element (tuple): A tuple containing:
            - date (str): The key (e.g., "Date").
            - records (list): A list of records with the same key.

    Yields:
        - Unique records as-is.
        - Duplicate records tagged with "invalid" and annotated with an error message.

    Example:
        Input: ("2023-12-31", [{"Date": "2023-12-31", "Euro_Price": 1234.56}, {"Date": "2023-12-31", "Euro_Price": 1234.56}])
        Output:
            - Unique: {"Date": "2023-12-31", "Euro_Price": 1234.56}
            - Invalid: {"Date": "2023-12-31", "Euro_Price": 1234.56, "Errors": "Duplicate record for date 2023-12-31"}
    """

    def process(self, element):
        date, records = element
        # Yield the first record as unique
        yield records[0]
        # Yield all subsequent records as invalid
        for record in records[1:]:
            logging.warning(f"Duplicate record found for date {date}: {record}")
            record["Errors"] = f"Duplicate record for date {date}"
            yield beam.pvalue.TaggedOutput("invalid", record)


#############
# Pipeline #
###########


def run():
    logging.info("Pipeline is starting...")
    with beam.Pipeline(options=PIPELINE_OPTIONS) as p:
        records = (
            p
            | "Read CSV"
            >> beam.io.ReadFromText(
                "gs://raw-historic-data/Daily_Prices_Home.csv", skip_header_lines=1
            )
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
            table="cocoa-prices-430315:cocoa_related.cocoa",
            schema="Date:DATE, Euro_Price:FLOAT",
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location="gs://cocoa-prices-temp-for-bq",
        )

        all_invalid_records | "Write Invalid to BigQuery" >> beam.io.WriteToBigQuery(
            table="cocoa-prices-430315:cocoa_related.invalid_cocoa",
            schema="Date:STRING, Euro_Price:STRING, Errors:STRING",
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location="gs://cocoa-prices-temp-for-bq",
        )


if __name__ == "__main__":
    run()
