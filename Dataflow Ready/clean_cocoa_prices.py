from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse

# Function to parse each line
def parse_csv(line):
    row = line.split(",")
    return {
        "Date": row[0].strip('"'),  # Remove extra quotation marks
        "Euro_Price": row[4]
        .strip('"')
        .replace(",", ""),  # Remove extra quotation marks and commas
    }


# Function to clean and convert data types
def clean_and_convert(element):
    # Convert the 'Date' field to datetime
    element["Date"] = datetime.strptime(element["Date"], "%d/%m/%Y")

    # Convert 'Euro_Price' to float
    try:
        element["Euro_Price"] = float(element["Euro_Price"])
    except ValueError:
        element["Euro_Price"] = None

    return element


# Function to filter out rows with missing prices
def filter_missing_prices(element):
    return element["Euro_Price"] is not None


# Function to format the output as CSV
def format_to_csv(element):
    return ",".join([element["Date"].strftime("%Y-%m-%d"), str(element["Euro_Price"])])


# Define the Beam pipeline
def run():
    pipeline_options = PipelineOptions()

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "Read CSV"
            >> beam.io.ReadFromText(
                "RAW/Daily Prices_Home_NEW.csv", skip_header_lines=1
            )
            | "Parse CSV" >> beam.Map(parse_csv)
            | "Clean and Convert" >> beam.Map(clean_and_convert)
            | "Filter Missing Prices" >> beam.Filter(filter_missing_prices)
            | "Format to CSV" >> beam.Map(format_to_csv)
            | "Write CSV"
            >> beam.io.WriteToText(
                "TEST/cleaned_cocoa_prices",
                file_name_suffix=".csv",
                header="Date,Euro_Price",
            )
        )


if __name__ == "__main__":
    run()
