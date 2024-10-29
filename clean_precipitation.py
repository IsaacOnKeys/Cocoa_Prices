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
            "precipitation": float(row[2]),
            "soil_moisture": float(row[3]),
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
        "precipitation": element.get("precipitation"),
        "soil_moisture": element.get("soil_moisture"),
    }


# Filter out rows with missing data
def filter_missing_data(element):
    return (
        element is not None
        and element.get("precipitation") is not None
        and element.get("soil_moisture") is not None
        and element.get("date") is not None
    )


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
        required_fields = ["date", "precipitation", "soil_moisture"]
        if not all(field in element for field in required_fields):
            valid = False
            missing = [field for field in required_fields if field not in element]
            errors.append(f"Missing fields: {', '.join(missing)}")

        # Data Type Validation
        date_str = element.get("date")
        precipitation = element.get("precipitation")
        soil_moisture = element.get("soil_moisture")

        # Validate date format
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
        if date_obj:
            if not (self.start_date <= date_obj <= self.end_date):
                valid = False
                errors.append("Date out of range (2014-01-01 to 2023-12-31)")

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
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

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
            >> beam.ParDo(filter_unique_dates).with_outputs("invalid", main="valid_unique")
        )

        # Write valid records to BigQuery
        (
            unique_records.valid_unique
            | "Write Valid to BigQuery"
            >> beam.io.WriteToBigQuery(
                table="cocoa-prices-430315:cocoa_prices.precipitation",
                schema="date:DATE, precipitation:FLOAT, soil_moisture:FLOAT",
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # Combine all invalid records from validation and uniqueness checks
        invalid_records = (validated_records.invalid, unique_records.invalid) | "Combine Invalid Records" >> beam.Flatten()

        # Write invalid records to BigQuery
        (
            invalid_records
            | "Write Invalid to BigQuery"
            >> beam.io.WriteToBigQuery(
                table="cocoa-prices-430315:cocoa_prices.invalid_precipitation",
                schema="date:DATE, precipitation:FLOAT, soil_moisture:FLOAT, Errors:STRING",
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

if __name__ == "__main__":
    run()
