import json
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def extract_and_clean(file_content):
    data = json.loads(file_content)

    observations = data.get("observations", [])

    for obs in observations:
        date = obs.get("date")
        value = obs.get("value")

        # replace invalid values with None
        if value == ".":
            value = None
        else:
            try:
                value = float(value)
            except ValueError:
                value = None

        yield {"date": date, "brent_price_eu": value}


class ValidateAndTransform(beam.DoFn):
    def __init__(self):
        super(ValidateAndTransform, self).__init__()
        self.start_date = datetime(2014, 1, 1)
        self.end_date = datetime(2023, 12, 31)

    def process(self, element):
        valid = True
        errors = []
        result = {}

        # Schema validation
        if "date" not in element or "brent_price_eu" not in element:
            valid = False
            errors.append("Missing 'date' or 'brent_price_eu' field")

        # Date validation
        try:
            date_obj = datetime.strptime(element["date"], "%Y-%m-%d")
            result["date"] = date_obj
            if not (self.start_date <= date_obj <= self.end_date):
                valid = False
                errors.append("Date out of range")
        except (ValueError, TypeError):
            valid = False
            errors.append("Invalid date format")

        # Brent price validation
        brent_price = element.get("brent_price_eu")
        if brent_price is None:
            valid = False
            errors.append("Missing Brent_Price")
        else:
            if not isinstance(brent_price, float):
                valid = False
                errors.append("Brent_Price is not a float")

        if valid:
            yield {
                "date": result["date"].strftime("%Y-%m-%d"),
                "brent_price_eu": brent_price,
            }
        else:
            element["Errors"] = "; ".join(errors)
            yield beam.pvalue.TaggedOutput("invalid", element)


# Check for uniqueness
class CheckUniqueness(beam.DoFn):
    def process(self, element):
        date, records = element
        if len(records) > 1:
            for record in records:
                record["Errors"] = f"Duplicate record for date {date}"
                yield beam.pvalue.TaggedOutput("invalid", record)
        else:
            yield records[0]


# Format the data to CSV
def format_to_csv(element):
    date = element["date"]
    brent_price_eu = element["brent_price_eu"]

    if brent_price_eu is None:
        brent_price_eu = ""

    return f"{date},{brent_price_eu}"


# Format invalid records to CSV
def format_invalid_to_csv(element):
    date = element.get("date", "")
    brent_price_eu = element.get("brent_price_eu", "")
    errors = element.get("Errors", "")

    return f"{date},{brent_price_eu},{errors}"


def run():
    pipeline_options = PipelineOptions()
    options = pipeline_options.view_as(StandardOptions)
    options.runner = "DirectRunner"

    with beam.Pipeline(options=pipeline_options) as p:
        validated_records = (
            p
            | "Read JSON" >> beam.io.ReadFromText("RAW/brent_oil_fred.json")
            | "Combine Lines"
            >> beam.CombineGlobally(lambda lines: "\n".join(lines)).without_defaults()
            | "Extract and Clean" >> beam.FlatMap(extract_and_clean)
            | "Validate and Transform"
            >> beam.ParDo(ValidateAndTransform()).with_outputs("invalid", main="valid")
        )

        valid_records = validated_records.valid
        invalid_records = validated_records.invalid

        # Setup for uniqueness check
        grouped_by_date = (
            valid_records
            | "Group by Date"
            >> beam.Map(lambda x: (x["date"], x))  # Map to (key, value)
            | "Group ByKey" >> beam.GroupByKey()
        )

        # Apply uniqueness check
        final_output = grouped_by_date | "Check Uniqueness" >> beam.ParDo(
            CheckUniqueness()
        ).with_outputs("invalid", main="unique")

        unique_records = final_output.unique
        invalid_duplicates = final_output.invalid

        # Write valid records to CSV
        (
            unique_records
            | "Format Unique to CSV" >> beam.Map(format_to_csv)
            | "Write Unique CSV"
            >> beam.io.WriteToText(
                "TEST/brent_prices_cleaned",
                file_name_suffix=".csv",
                header="date,brent_price_eu",
            )
        )

        # Write invalid records (side-output) to a separate CSV
        invalid_records = (
            invalid_records,
            invalid_duplicates,
        ) | "Combine Invalid Records" >> beam.Flatten()

        (
            invalid_records
            | "Format Invalid to CSV" >> beam.Map(format_invalid_to_csv)
            | "Write Invalid CSV"
            >> beam.io.WriteToText(
                "TEST/brent_prices_invalid",
                file_name_suffix=".csv",
                header="date,brent_price_eu,Errors",
            )
        )


if __name__ == "__main__":
    run()
