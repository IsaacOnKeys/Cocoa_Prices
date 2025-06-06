"""
Usage:
To run with DirectRunner (default):

docker run --rm -v /c/Users/iamno/AppData/Roaming/gcloud:/root/.config/gcloud \
     europe-west3-docker.pkg.dev/cocoa-prices-430315/cocoa-code-project/dataflow-pipelines-batch:latest \
    clean_precipitation.py \
    --runner=DirectRunner \
    --project=cocoa-prices-430315 \
    --region=europe-west3 \
    --temp_location=gs://raw-historic-data/temp \
    --staging_location=gs://raw-historic-data/staging \
    --save_main_session

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

import logging
import os
import time
from datetime import datetime, timedelta

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

if RUNNER not in ["DirectRunner", "DataflowRunner"]:
    raise ValueError(f"Unsupported runner: {STANDARD_OPTIONS.runner}")
logging.info(f"Runner: {RUNNER}")

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

    row = line.split(",")
    try:

        if len(row) < 4:
            raise ValueError(f"Row does not have enough fields: {row}")
        """
        Parse and handle missing data
        The value "-999" is specified in the dataset documentation as a placeholder for missing data.
        Reference: Dataset Header - "The value for missing source data that cannot be computed or is outside of the source's availability range: -999"
        """
        precipitation = float(row[2]) if row[2] != "-999" else None
        soil_moisture = float(row[3]) if row[3] != "-999" else None

        parsed = {
            "YEAR": int(row[0]),
            "DOY": int(row[1]),
            "precipitation": precipitation,
            "soil_moisture": soil_moisture,
        }

        logging.info(f"Parsed line successfully: {parsed}")
        return parsed
    except (ValueError, TypeError) as e:
        logging.warning(f"Skipping line due to parsing error: {line}, Error: {e}")
        return None


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
        return None

    year = element.get("YEAR")
    doy = element.get("DOY")

    try:
        if year is None or doy is None:
            raise ValueError(f"Invalid YEAR or DOY: {element}")
        date = datetime(year, 1, 1) + timedelta(days=doy - 1)
        date_str = date.strftime("%Y-%m-%d")
    except (ValueError, TypeError) as e:
        logging.warning(f"Skipping element due to date error: {element}, Error: {e}")
        return None

    return {
        "date": date_str,
        "precipitation": element.get("precipitation"),
        "soil_moisture": element.get("soil_moisture"),
    }


def filter_missing_data(element):
    """
    Filters out rows with missing data fields.

    Args:
        element (dict): Input data to validate.

    Returns:
        bool: True if the data is complete, False otherwise.
    """
    if (
        element is not None
        and element.get("precipitation") is not None
        and element.get("soil_moisture") is not None
        and element.get("date") is not None
    ):
        return True
    logging.warning(f"Filtering out missing data: {element}")
    return False


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
        self.end_date = datetime(2024, 12, 31)
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
    """
    Filters out duplicate records based on the 'date' key.

    Args:
        element (tuple): A tuple where the first item is the 'date' key and the second is a list of records.

    Yields:
        dict: A unique record if only one exists for the date.
        beam.pvalue.TaggedOutput: Tagged invalid records for duplicate dates.
    """
    date, records = element
    records = list(records)
    if len(records) == 1:
        yield records[0]
    else:
        for record in records:
            record["Errors"] = "Duplicate date"
            yield beam.pvalue.TaggedOutput("invalid", record)

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
