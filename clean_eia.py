import csv
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


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
    return element["Brent_Price"] is not None


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
            >> beam.io.ReadFromText("RAW/eiaHistoricData.csv", skip_header_lines=1)
            | "Parse CSV" >> beam.Map(parse_csv)
            | "Clean and Convert" >> beam.Map(clean_and_convert)
            | "Filter Missing Prices" >> beam.Filter(filter_missing_prices)
            | "Format to CSV" >> beam.Map(format_to_csv)
            | "Write CSV"
            >> beam.io.WriteToText(
                "TEST/eia_clean.csv",
                file_name_suffix=".csv",
                header="date,Brent_Price",
            )
        )


if __name__ == "__main__":
    run()
