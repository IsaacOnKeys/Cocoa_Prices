"""Isolated historical backfill for NASA POWER daily CSV.

Requires an explicit held input generation/hash, date interval and candidate
dataset. See README.md and scripts/operations/backfill_history.py create.
No bare invocation or production WRITE_TRUNCATE destination is supported.
"""

import logging
import math
import os
import time
from datetime import datetime, timedelta

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
    Parses a CSV line into a structured dictionary.

    The value "-999" is specified in the dataset documentation as a placeholder for missing data.
    Reference: Dataset Header - "The value for missing source data that cannot be computed or is
    outside of the source's availability range: -999".

    Args:
        line (str): A single line from the CSV file.

    Returns:
        dict: Parsed data with keys 'YEAR', 'DOY', 'precipitation', and 'soil_moisture',
              or None if the line fails validation.

    Raises:
        ValueError: If the line does not have enough fields or contains invalid data.
    """

    # NASA's declared metadata is protocol text, not a rejected observation.
    # Unknown text still goes to quarantine; do not filter all nonnumeric lines.
    metadata = ('NASA/POWER Source Native Resolution Daily Data', 'Dates (month/day/year):',
                'Location: latitude ', 'elevation from MERRA-2:', 'The value for missing source data ',
                'parameter(s):', 'PRECTOTCORR ', 'GWETROOT ')
    if line.strip() in {'-BEGIN HEADER-', '-END HEADER-', 'YEAR,DOY,PRECTOTCORR,GWETROOT'} or line.startswith(metadata):
        return {'_metadata': True}
    row = line.split(",")
    try:

        if len(row) < 4:
            raise ValueError(f"Row does not have enough fields: {row}")
        """
        Parse and handle missing data
        The value "-999" is specified in the dataset documentation as a placeholder for missing data.
        Reference: Dataset Header - "The value for missing source data that cannot be computed or is outside of the source's availability range: -999"
        """
        precipitation = float(row[2])
        soil_moisture = float(row[3])
        precipitation = None if precipitation <= -999 else precipitation
        soil_moisture = None if soil_moisture <= -999 else soil_moisture

        parsed = {
            "YEAR": int(row[0]),
            "DOY": int(row[1]),
            "precipitation": precipitation,
            "soil_moisture": soil_moisture,
        }

        parsed['_raw_line'] = line
        return parsed
    except (ValueError, TypeError):
        return {'_parse_error': 'malformed_csv', '_raw_line': line}


def clean_and_transform(element):
    """
    Cleans and transforms raw parsed data into a final structured format.

    Args:
        element (dict): Parsed data with keys 'YEAR', 'DOY', 'precipitation', and 'soil_moisture'.

    Returns:
        dict: Transformed data with a 'date' key and cleaned precipitation and soil_moisture values,
              or None if the input element is invalid.
    """
    if element is None:
        raise ValueError('Missing historical weather input')
    if element.get('_parse_error'):
        return element

    year = element.get("YEAR")
    doy = element.get("DOY")

    try:
        if year is None or doy is None:
            raise ValueError(f"Invalid YEAR or DOY: {element}")
        date = datetime(year, 1, 1) + timedelta(days=doy - 1)
        if doy < 1 or date.year != year:
            raise ValueError('Invalid day of year')
        date_str = date.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return {**element, '_parse_error': 'invalid_year_or_day'}

    return {
        "date": date_str,
        "precipitation": element.get("precipitation"),
        "soil_moisture": element.get("soil_moisture"),
        "_raw_line": element.get("_raw_line"),
    }


def filter_missing_data(element):
    """
    Filters out rows with missing data fields.

    Args:
        element (dict): Input data to validate.

    Returns:
        bool: True if the data is complete, False otherwise.
    """
    if element is None:
        raise ValueError('Missing historical weather input')
    return True  # Explicit missing measurements must reach the invalid-record sink.


class ValidateAndTransform(beam.DoFn):
    """
    A DoFn class for validating and transforming records into valid or invalid outputs.

    Attributes:
        start_date (datetime): The earliest allowed date for validation.
        end_date (datetime): The latest allowed date for validation.
    """

    def __init__(self):
        """
        Initializes validation rules and date range for the transformation.
        """
        super(ValidateAndTransform, self).__init__()
        self.start_date = datetime(2014, 1, 1)
        self.end_date = datetime(2025, 12, 31)
        self.__module__ = "__main__"

    def process(self, element):
        """
        Validates and transforms a single record.

        This method performs schema validation, data type validation, range checks, and missing
        value handling for a given record. If the record passes all validations, it yields a valid
        record. If any validation fails, it tags the record as "invalid" and attaches a list of
        validation errors.

        Validation Steps:
            1. Schema Validation:
            - Ensures all required fields ('date', 'precipitation', 'soil_moisture') are present.
            2. Data Type Validation:
            - Ensures 'precipitation' and 'soil_moisture' are floats.
            - Ensures 'date' is a valid string in "YYYY-MM-DD" format.
            3. Range Validation:
            - Ensures 'date' is within the specified range (2014-01-01 to 2024-12-31).
            4. Missing Value Handling:
            - Flags 'precipitation' or 'soil_moisture' as invalid if values are missing.

        Args:
            element (dict): A dictionary representing a single record to validate and transform.
                            Expected keys: 'date', 'precipitation', 'soil_moisture'.

        Yields:
            dict: A valid record if all validation checks pass. Includes:
                - 'date' (str): Validated date in "YYYY-MM-DD" format.
                - 'precipitation' (float): Validated precipitation value.
                - 'soil_moisture' (float): Validated soil moisture value.

            beam.pvalue.TaggedOutput: A tagged invalid record with:
                - The original record.
                - A string listing validation errors under the key "Errors".

        Raises:
            ValueError: Raised internally for invalid fields during type conversion or date parsing.

        Example:
            Input: {"date": "2023-12-15", "precipitation": 12.5, "soil_moisture": 0.8}
            Output: Yields {"date": "2023-12-15", "precipitation": 12.5, "soil_moisture": 0.8}

            Input: {"date": "invalid-date", "precipitation": "N/A", "soil_moisture": 0.8}
            Output: Yields a tagged output with validation errors.
        """
        if element.get('_parse_error'):
            yield beam.pvalue.TaggedOutput('invalid', element)
            return
        valid = True
        errors = []

        required_fields = ["date", "precipitation", "soil_moisture"]
        if not all(field in element for field in required_fields):
            valid = False
            missing = [field for field in required_fields if field not in element]
            errors.append(f"Missing fields: {', '.join(missing)}")

        date_str = element.get("date")
        precipitation = element.get("precipitation")
        soil_moisture = element.get("soil_moisture")

        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            valid = False
            errors.append("Invalid date format")
            date_obj = None

        # Validate numerical fields
        if precipitation is not None and not isinstance(precipitation, float):
            valid = False
            errors.append("precipitation is not a float")
        if soil_moisture is not None and not isinstance(soil_moisture, float):
            valid = False
            errors.append("soil_moisture is not a float")

        if isinstance(precipitation, float) and (not math.isfinite(precipitation) or precipitation < 0):
            valid = False
            errors.append('Invalid precipitation measurement')
        if isinstance(soil_moisture, float) and (not math.isfinite(soil_moisture) or not 0 <= soil_moisture <= 1):
            valid = False
            errors.append('Invalid soil moisture measurement')

        # Date Range Validation
        if date_obj and not (self.start_date <= date_obj <= self.end_date):
            valid = False
            errors.append("Date out of range (2014-01-01 to 2024-12-31)")

        # Missing Value Handling
        if precipitation is None:
            valid = False
            errors.append("Missing precipitation value")
        if soil_moisture is None:
            valid = False
            errors.append("Missing soil_moisture value")

        if valid:
            yield {
                "date": date_str,
                "precipitation": precipitation,
                "soil_moisture": soil_moisture,
            }
        else:
            element["Errors"] = "; ".join(errors)
            yield beam.pvalue.TaggedOutput("invalid", element)


def key_by_date(record):
    """
    Creates a key-value pair using the record's date as the key.

    Args:
        record (dict): A record containing a 'date' key.

    Returns:
        tuple: A tuple with the 'date' as the key and the record as the value.

    Raises:
        ValueError: If the record does not contain a 'date' key.
    """
    if "date" not in record:
        logging.error(f"Missing 'date' in record: {record}")
        return None
    return (record["date"], record)


def filter_unique_dates(element):
    """Collapse identical source facts and quarantine every conflicting date revision."""
    _, records = element
    unique, invalid = resolve_duplicates(records, 'weather')
    yield from unique
    for row in invalid:
        yield beam.pvalue.TaggedOutput('invalid', row)



# Check valid records before writing to BigQuery
def check_valid_record(record):
    """
    Checks if a record is valid before writing to BigQuery.

    Args:
        record (dict): A record to validate.

    Returns:
        dict: The same record if validation passes.

    Raises:
        ValueError: If the record contains invalid data.
    """
    if (
        record["date"] is None
        or record["precipitation"] is None
        or record["soil_moisture"] is None
    ):
        raise ValueError(f"Invalid record: {record}")
    return record


#############
# Pipeline #
###########


def run():
    request, options = configure('weather')
    configure_worker_package(options, __file__)
    logging.info("Pipeline is starting...")
    logging.info("Runner = %s", options.view_as(StandardOptions).runner)
    with beam.Pipeline(options=options) as p:
        parsed_records = (
            p
            | "Read CSV"
            >> beam.io.ReadFromText(
                request.input_uri,
                skip_header_lines=0,
            )
            | "Parse CSV" >> beam.Map(parse_csv)
            | "Exclude Declared Metadata" >> beam.Filter(lambda record: not record.get('_metadata'))
            | "Clean & Transform" >> beam.Map(clean_and_transform)
            | "Filter Missing Data" >> beam.Filter(filter_missing_data)
        )

        validated_records = (
            parsed_records
            | "Extract and Clean" >> beam.FlatMap(lambda x: [x])
            | "Validate and Transform"
            >> beam.ParDo(ValidateAndTransform()).with_outputs("invalid", main="valid")
        )

        valid_records = validated_records.valid | "Select Requested Interval" >> beam.Filter(in_interval, request)
        invalid_records = validated_records.invalid

        # ── deduplicate by date ─────────────────────────────────────────────
        unique_records = (
            valid_records
            | "Filter rows with date"
            >> beam.Filter(
                lambda r: r and r.get("date")  # keep only dicts that have a date
            )
            | "Key by date" >> beam.Map(lambda r: (r["date"], r))
            | "Reshuffle" >> beam.Reshuffle()
            # **hard guard** – only tuples reach GroupByKey
            | "Keep KV tuples"
            >> beam.Filter(lambda kv: isinstance(kv, tuple) and len(kv) == 2)
            | "Group by date" >> beam.GroupByKey()
            | "Filter unique dates"
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
                table=request.table(),
                schema="date:DATE, precipitation:FLOAT, soil_moisture:FLOAT",
                write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=f"gs://cocoa-prices-temp-for-bq/backfill/{request.token}/weather/",
            )
        )

        combined_invalid_records = [
            invalid_records,
            unique_records.invalid,
        ] | "Combine Invalid Records" >> beam.Flatten()

        evidence = combined_invalid_records | "Retain Invalid Evidence" >> beam.Map(invalid_record, 'weather', request)
        (
            evidence | "Write Invalid to BigQuery"
            >> beam.io.WriteToBigQuery(
                table=request.table(invalid=True),
                schema="date:STRING, precipitation:STRING, soil_moisture:STRING, Errors:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                custom_gcs_temp_location=f"gs://cocoa-prices-temp-for-bq/backfill/{request.token}/weather/",
            )
        )
        evidence | "Collect Rejections" >> beam.combiners.ToList() | "Archive Rejections" >> beam.Map(persist_invalid, request)
    finish(request, p.result)


if __name__ == "__main__":
    run()
