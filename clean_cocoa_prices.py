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
options.view_as(StandardOptions).runner = "DirectRunner"  #'DataflowRunner'


def parse_csv(line):
    reader = csv.reader([line])
    row = next(reader)
    return {
        "Date": row[0].strip('"'),
        "Euro_Price": row[4].strip('"'),
    }


class ValidateAndTransform(beam.DoFn):
    def process(self, element):
        from datetime import datetime

        valid = True
        errors = []

        # Validate and transform 'Date'
        try:
            element["Date"] = datetime.strptime(element["Date"], "%d/%m/%Y")
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


# Format the output as CSV
def format_to_csv(element):
    return ",".join([element["Date"].strftime("%Y-%m-%d"), str(element["Euro_Price"])])


# Format invalid records as CSV
def format_invalid_to_csv(element):
    date = element.get("Date", "")
    if isinstance(date, datetime):
        date = date.strftime("%Y-%m-%d")
    euro_price = element.get("Euro_Price", "")
    errors = element.get("Errors", "")
    return ",".join([str(date), str(euro_price), errors])


def run():
    pipeline_options = PipelineOptions()

    with beam.Pipeline(options=pipeline_options) as p:
        records = (
            p
            | "Read CSV"
            >> beam.io.ReadFromText(
                "RAW/Daily Prices_Home.csv",
                skip_header_lines=1,
            )
            | "Parse CSV" >> beam.Map(parse_csv)
        )

        validated_records = records | "Validate and Transform" >> beam.ParDo(
            ValidateAndTransform()
        ).with_outputs("invalid", main="valid")

        valid_records = validated_records.valid
        invalid_records = validated_records.invalid

        # Write valid records to CSV
        (
            valid_records
            | "Format Valid to CSV" >> beam.Map(format_to_csv)
            | "Write Valid CSV"
            >> beam.io.WriteToText(
                "TEST/cocoa_valid",
                file_name_suffix=".csv",
                header="Date,Euro_Price",
            )
        )
        # Side-output invalid records to CSV
        (
            invalid_records
            | "Format Invalid to CSV" >> beam.Map(format_invalid_to_csv)
            | "Write Invalid CSV"
            >> beam.io.WriteToText(
                "TEST/cocoa_invalid",
                file_name_suffix=".csv",
                header="Date,Euro_Price,Errors",
            )
        )


if __name__ == "__main__":
    run()
