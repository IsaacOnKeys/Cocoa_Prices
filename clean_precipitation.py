import logging
from datetime import datetime, timedelta

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    StandardOptions,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def parse_csv(line):
    row = line.split(",")
    try:
        parsed = {
            "YEAR": int(row[0]),
            "DOY": int(row[1]),
            "PRECTOTCORR": float(row[2]),
            "GWETROOT": float(row[3]),
        }
        logging.info(f"Parsed line successfully: {parsed}")
        return parsed
    except ValueError as e:
        logging.warning(f"Skipping line due to parsing error: {line}, Error: {e}")
        return None


# Clean and parse data types
def clean_and_transform(element):
    if element is None:
        return None

    # Combine YEAR and DOY to create a date
    year = element.get("YEAR")
    doy = element.get("DOY")
    try:
        date = datetime(year, 1, 1) + timedelta(days=doy - 1)
        date_str = date.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        date_str = None

    return {
        "date": date_str,
        "PRECTOTCORR": element.get("PRECTOTCORR"),
        "GWETROOT": element.get("GWETROOT"),
    }


# Filter out rows with missing data
def filter_missing_data(element):
    return (
        element is not None
        and element.get("PRECTOTCORR") is not None
        and element.get("GWETROOT") is not None
        and element.get("date") is not None
    )


# Format the output as CSV for valid records
def format_valid_to_csv(element):
    return ",".join(
        [
            element["date"],
            f"{element['PRECTOTCORR']}",
            f"{element['GWETROOT']}",
        ]
    )


# Format invalid records as CSV
def format_invalid_to_csv(element):
    date = element.get("date", "")
    prectotcorr = element.get("PRECTOTCORR", "")
    gwetroot = element.get("GWETROOT", "")
    errors = element.get("Errors", "")

    return ",".join([str(date), str(prectotcorr), str(gwetroot), str(errors)])


# DoFn for Validation and Transformation
class ValidateAndTransform(beam.DoFn):
    def __init__(self):
        super(ValidateAndTransform, self).__init__()
        self.start_date = datetime(2014, 1, 1)
        self.end_date = datetime(2023, 12, 31)

    def process(self, element):
        valid = True
        errors = []

        # Schema Validation
        required_fields = ["date", "PRECTOTCORR", "GWETROOT"]
        if not all(field in element for field in required_fields):
            valid = False
            missing = [field for field in required_fields if field not in element]
            errors.append(f"Missing fields: {', '.join(missing)}")

        # Data Type Validation
        date_str = element.get("date")
        prectotcorr = element.get("PRECTOTCORR")
        gwetroot = element.get("GWETROOT")

        # Validate date format
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            valid = False
            errors.append("Invalid date format")
            date_obj = None

        # Validate numerical fields
        if prectotcorr is not None and not isinstance(prectotcorr, float):
            valid = False
            errors.append("PRECTOTCORR is not a float")
        if gwetroot is not None and not isinstance(gwetroot, float):
            valid = False
            errors.append("GWETROOT is not a float")

        # Date Range Validation
        if date_obj:
            if not (self.start_date <= date_obj <= self.end_date):
                valid = False
                errors.append("Date out of range (2014-01-01 to 2023-12-31)")
        # Missing Value Handling
        if prectotcorr is None:
            valid = False
            errors.append("Missing PRECTOTCORR value")
        if gwetroot is None:
            valid = False
            errors.append("Missing GWETROOT value")
        if valid:
            yield {
                "date": date_str,
                "PRECTOTCORR": prectotcorr,
                "GWETROOT": gwetroot,
            }
        else:
            element["Errors"] = "; ".join(errors)
            yield beam.pvalue.TaggedOutput("invalid", element)


# Uniqueness checks for GroupByKey
def key_by_date(record):
    return (record["date"], record)


def filter_unique_dates(element):
    date, records = element
    records = list(records)
    if len(records) == 1:
        yield records[0]
    else:
        for record in records:
            record["Errors"] = "Duplicate date"
            yield beam.pvalue.TaggedOutput("invalid", record)


def run():
    pipeline_options = PipelineOptions()
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    gcp_options.project = "cocoa-prices-430315"
    gcp_options.job_name = "cleaning-weather-data"
    gcp_options.staging_location = "gs://raw_historic_data/staging"
    gcp_options.temp_location = "gs://raw_historic_data/temp"
    pipeline_options.view_as(StandardOptions).runner = (
        "DirectRunner"  # Use 'DataflowRunner' for cloud execution
    )

    with beam.Pipeline(options=pipeline_options) as p:
        parsed_records = (
            p
            | "Read CSV"
            >> beam.io.ReadFromText(
                "RAW/POWER_Point_Daily.csv",
                # "gs://raw_historic_data/POWER_Point_Daily.csv",
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

        # Write valid records to CSV in cleaned-coca-data
        (
            unique_records.valid_unique
            | "Format Valid to CSV" >> beam.Map(format_valid_to_csv)
            | "Write Valid CSV"
            >> beam.io.WriteToText(
                "TEST/weather_data_cleaned",
                # "gs://cleaned-coca-data/weather_data_cleaned",
                file_name_suffix=".csv",
                header="date,PRECTOTCORR,GWETROOT",
            )
        )
        # Write invalid records to side-output: weather_data_invalid
        (
            unique_records.invalid
            | "Format Invalid to CSV" >> beam.Map(format_invalid_to_csv)
            | "Write Invalid CSV"
            >> beam.io.WriteToText(
                "TEST/weather_data_invalid",
                # "gs://cleaned-coca-data/weather_data_invalid",
                file_name_suffix=".csv",
                header="date,PRECTOTCORR,GWETROOT,Errors",
            )
        )


if __name__ == "__main__":
    run()
