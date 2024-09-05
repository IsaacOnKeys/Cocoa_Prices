import argparse
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
gcp_options.job_name = "joined-cocoa-oil-weather"
gcp_options.staging_location = "gs://cleaned-coca-data/staging"
gcp_options.temp_location = "gs://cleaned-coca-data/temp"
options.view_as(StandardOptions).runner = "DirectRunner"  #'DataflowRunner'


# Function to parse the cocoa price CSV
def parse_cocoa_csv(line):
    date, price = line.split(",")
    return (date, {"date": date, "cocoa_price_euro": float(price)})


# Function to parse the oil prices CSV
def parse_oil_csv(line):
    date, brent_price = line.split(",")
    return (date, {"date": date, "Brent_Price": float(brent_price)})


# Function to parse the precipitation CSV
def parse_precipitation_csv(line):
    date, precipitation, soil_moisture = line.split(",")
    return (
        date,
        {
            "date": date,
            "precipitation": float(precipitation),
            "soil_moisture": float(soil_moisture),
        },
    )


# Function to join the records by date, ensuring only dates from cocoa are retained
def join_records(cocoa_record, oil_records, precipitation_records):
    combined = cocoa_record

    if oil_records:
        combined.update(oil_records[0])

    if precipitation_records:
        combined.update(precipitation_records[0])

    return combined


# Function to fill forward missing values
def fill_forward(records):
    last_record = None
    for record in records:
        if last_record:
            for key in record:
                if record[key] is None and last_record[key] is not None:
                    record[key] = last_record[key]
        last_record = record
        yield record


# Function to format the output as CSV
def format_to_csv(element):
    return ",".join(
        [
            element["date"],
            str(element.get("cocoa_price_euro", "")),
            str(element.get("Brent_Price", "")),
            str(element.get("precipitation", "")),
            str(element.get("soil_moisture", "")),
        ]
    )


# Define the Beam pipeline
def run():
    pipeline_options = PipelineOptions()

    with beam.Pipeline(options=pipeline_options) as p:
        cocoa = (
            p
            | "Read Cocoa Prices"
            >> beam.io.ReadFromText(
                "gs://cleaned-coca-data/cocoa_prices*.csv", skip_header_lines=1
            )
            | "Parse Cocoa CSV" >> beam.Map(parse_cocoa_csv)
        )

        oil = (
            p
            | "Read Oil Prices"
            >> beam.io.ReadFromText(
                "gs://cleaned-coca-data/oil_prices*.csv", skip_header_lines=1
            )
            | "Parse Oil CSV" >> beam.Map(parse_oil_csv)
        )

        precipitation = (
            p
            | "Read Precipitation"
            >> beam.io.ReadFromText(
                "gs://cleaned-coca-data/weather_data*.csv", skip_header_lines=1
            )
            | "Parse Precipitation CSV" >> beam.Map(parse_precipitation_csv)
        )

        # Left join cocoa prices with oil prices and precipitation
        combined = (
            {"cocoa": cocoa, "oil": oil, "precipitation": precipitation}
            | "Join Cocoa with Oil and Precipitation" >> beam.CoGroupByKey()
            | "Flatten All"
            >> beam.Map(
                lambda x: (
                    x[0],
                    join_records(
                        x[1]["cocoa"][0] if x[1]["cocoa"] else {},
                        x[1]["oil"],
                        x[1]["precipitation"],
                    ),
                )
            )
            # Filter out records where there is no cocoa data
            | "Filter Missing Cocoa Data"
            >> beam.Filter(lambda x: "cocoa_price_euro" in x[1])
            # Group by date for further processing
            | "Group by Date" >> beam.GroupByKey()
            # Flatten the records and fill forward missing values
            | "Fill Forward Missing Values"
            >> beam.FlatMap(
                lambda kv: fill_forward(sorted(kv[1], key=lambda r: r["date"]))
            )
            # Format the final output as CSV
            | "Format to CSV" >> beam.Map(format_to_csv)
            | "Write CSV"
            >> beam.io.WriteToText(
                "TEST/combined_cleaned_data.csv",
                file_name_suffix=".csv",
                header="date,cocoa_price_euro,Brent_Price,precipitation,soil_moisture",
            )
        )


if __name__ == "__main__":
    run()
