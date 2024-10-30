import csv
import logging
import time
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)

# Constants
PROJECT_ID = "cocoa-prices-430315"
STAGING_LOCATION = "gs://raw_historic_data/staging"
TEMP_LOCATION = "gs://raw_historic_data/temp"
SOURCE_FILE = "RAW/Daily Prices_Home.csv"
VALID_TABLE = f"{PROJECT_ID}:cocoa_prices.cocoa"
INVALID_TABLE = f"{PROJECT_ID}:cocoa_prices.invalid_cocoa"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

def parse_csv(line):
    reader = csv.reader([line])
    row = next(reader)
    return {
        "Date": row[0].strip('"'),
        "Euro_Price": row[4].strip('"'),
    }

class ValidateAndTransform(beam.DoFn):
    def process(self, element):
        valid = True
        errors = []

        # Validate and transform 'Date'
        try:
            date_obj = datetime.strptime(element["Date"], "%d/%m/%Y")
            element["Date"] = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            valid = False
            errors.append("Invalid date format")

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
            element["Errors"] = "; ".join(errors)
            yield beam.pvalue.TaggedOutput("invalid", element)

def run():
    pipeline_options = PipelineOptions()

    setup_options = pipeline_options.view_as(SetupOptions)
    setup_options.save_main_session = True
    setup_options.requirements_file = 'requirements.txt'    
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.runner = 'DataflowRunner'

    gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    gcp_options.project = PROJECT_ID
	# Ensure unique job name
    gcp_options.job_name = f"cleaning-cocoa-prices-data-{int(time.time())}"
    gcp_options.staging_location = STAGING_LOCATION
    gcp_options.temp_location = TEMP_LOCATION
    gcp_options.region = "europe-west3"

    with beam.Pipeline(options=pipeline_options) as p:
        records = (
            p
            | "Read CSV" >> beam.io.ReadFromText(SOURCE_FILE, skip_header_lines=1)
            | "Parse CSV" >> beam.Map(parse_csv)
        )

        validated_records = records | "Validate and Transform" >> beam.ParDo(
            ValidateAndTransform()
        ).with_outputs("invalid", main="valid")

        valid_records = validated_records.valid
        invalid_records = validated_records.invalid

        # Write valid records to BigQuery
        valid_records | "Write Valid to BigQuery" >> beam.io.WriteToBigQuery(
            table=VALID_TABLE,
            schema="Date:DATE, Euro_Price:FLOAT",
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

        # Side-output invalid records to BigQuery
        invalid_records | "Format Invalid to BigQuery" >> beam.io.WriteToBigQuery(
            table=INVALID_TABLE,
            schema="Date:STRING, Euro_Price:FLOAT",
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

if __name__ == "__main__":
    run()
