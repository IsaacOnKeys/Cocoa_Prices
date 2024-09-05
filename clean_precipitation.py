import argparse
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

options = PipelineOptions()
gcp_options = options.view_as(GoogleCloudOptions)
gcp_options.project = "cocoa-prices-430315"
gcp_options.job_name = "cleaning-weather-data"
gcp_options.staging_location = "gs://raw_historic_data/staging"
gcp_options.temp_location = "gs://raw_historic_data/temp"
options.view_as(StandardOptions).runner = "DirectRunner"  #'DataflowRunner'


# Function to parse each line
def parse_csv(line):
    row = line.split(",")
    return {
        "YEAR": int(row[0]),
        "DOY": int(row[1]),
        "PRECTOTCORR": row[2],
        "GWETROOT": row[3],
    }


# Function to clean and convert data types
def clean_and_convert(element):
    # Combine YEAR and DOY to create a date
    year = element["YEAR"]
    doy = element["DOY"]
    element["date"] = datetime(year, 1, 1) + timedelta(days=doy - 1)

    # Convert PRECTOTCORR and GWETROOT to float
    try:
        element["PRECTOTCORR"] = float(element["PRECTOTCORR"])
        element["GWETROOT"] = float(element["GWETROOT"])
    except ValueError:
        element["PRECTOTCORR"] = None
        element["GWETROOT"] = None

    return element


# Function to filter out rows with missing data
# def filter_missing_data(element):
#     return element["PRECTOTCORR"] is not None and element["GWETROOT"] is not None
def filter_missing_data(element):
    return (
        element is not None
        and element.get("PRECTOTCORR") is not None
        and element.get("GWETROOT") is not None
    )


# Function to format the output as CSV
def format_to_csv(element):
    return ",".join(
        [
            element["date"].strftime("%Y-%m-%d"),
            str(element["PRECTOTCORR"]),
            str(element["GWETROOT"]),
        ]
    )


# Define the Beam pipeline
def run():
    pipeline_options = PipelineOptions()

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read CSV"
            >> beam.io.ReadFromText(
                "gs://raw_historic_data/POWER_Point_Daily.csv", skip_header_lines=1
            )
            | "Parse CSV" >> beam.Map(parse_csv)
            | "Clean and Convert" >> beam.Map(clean_and_convert)
            | "Filter Missing Data" >> beam.Filter(filter_missing_data)
            | "Format to CSV" >> beam.Map(format_to_csv)
            | "Write CSV"
            >> beam.io.WriteToText(
                "gs://cleaned-coca-data/weather_data",
                file_name_suffix=".csv",
                header="date,PRECTOTCORR,GWETROOT",
            )
        )


if __name__ == "__main__":
    run()
