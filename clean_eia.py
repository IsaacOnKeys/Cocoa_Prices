import argparse
import csv
import logging
from datetime import datetime

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
gcp_options.job_name = "cleaning-oil-prices"
gcp_options.staging_location = "gs://raw_historic_data/staging"
gcp_options.temp_location = "gs://raw_historic_data/temp"
options.view_as(StandardOptions).runner = "DirectRunner"  #'DataflowRunner'


# Function to parse each line
def parse_csv(line):
    reader = csv.reader([line])
    for row in reader:
        return {"date": row[0], "Brent_Price": row[9]}


# Function to clean and convert data types
def clean_and_convert(element):
    # Convert the 'date' field to datetime
    element["date"] = datetime.strptime(element["date"], "%Y-%m-%d")

    # Convert 'Brent_Price' to float
    try:
        element["Brent_Price"] = float(element["Brent_Price"])
    except ValueError:
        element["Brent_Price"] = None

    return element


# Function to filter out rows with missing Brent_Price
def filter_missing_prices(element):
    return element is not None and element.get("Brent_Price") is not None


# Function to format the output as CSV
def format_to_csv(element):
    return ",".join(
        [
            element["date"].strftime("%Y-%m-%d"),
            str(element["Brent_Price"]),
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
                "gs://raw_historic_data/eiaHistoricData.csv ", skip_header_lines=1
            )
            | "Parse CSV" >> beam.Map(parse_csv)
            | "Clean and Convert" >> beam.Map(clean_and_convert)
            | "Filter Missing Prices" >> beam.Filter(filter_missing_prices)
            | "Format to CSV" >> beam.Map(format_to_csv)
            | "Write CSV"
            >> beam.io.WriteToText(
                "gs://cleaned-coca-data/oil_prices",
                file_name_suffix=".csv",
                header="date,Brent_Price",
            )
        )


if __name__ == "__main__":
    run()
