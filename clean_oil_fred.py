import json
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


def extract_and_clean(file_content):
    data = json.loads(file_content)

    observations = data.get("observations", [])
 
    for obs in observations:
        date = obs.get("date")
        value = obs.get("value")

        # replace invalid values with None
        try:
            value = float(value) if value != "." else None
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

def run():
    pipeline_options = PipelineOptions()
    gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    gcp_options.project = "cocoa-prices-430315"
    gcp_options.job_name = "cleaning-oil-data"
    gcp_options.staging_location = "gs://raw_historic_data/staging"
    gcp_options.temp_location = "gs://raw_historic_data/temp"
    pipeline_options.view_as(StandardOptions).runner = (
        'DataflowRunner'
    )
    with beam.Pipeline(options=pipeline_options) as p:
        validated_records = (
            p
            | "Read JSON" >> beam.io.ReadFromText("gs://raw_historic_data/brent_oil_fred.json")
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
            >> beam.Map(lambda x: (x["date"], x))
            | "Group ByKey" >> beam.GroupByKey()
        )

        # Apply uniqueness check
        final_output = grouped_by_date | "Check Uniqueness" >> beam.ParDo(
            CheckUniqueness()
        ).with_outputs("invalid", main="unique")

        unique_records = final_output.unique
        invalid_duplicates = final_output.invalid

        # Write valid records to BigQuery
        (
            unique_records
            | "Write Valid to BigQuery"
            >> beam.io.WriteToBigQuery(
                table="cocoa-prices-430315:cocoa_prices.brent_prices",
                schema="date:DATE, brent_price_eu:FLOAT",
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

       # Side-output invalid records to BigQuery
        
        invalid_records = (
            invalid_records,
            invalid_duplicates,
        ) | "Combine Invalid Records" >> beam.Flatten()

        (
            invalid_records
            | "Format Invalid to BigQuery"
             >> beam.io.WriteToBigQuery(                
                table="cocoa-prices-430315:cocoa_prices.invalid_brent_prices",
                schema="Date:DATE, brent_price_eu:FLOAT",
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

if __name__ == "__main__":
    run()
