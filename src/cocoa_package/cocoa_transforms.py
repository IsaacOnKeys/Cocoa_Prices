import csv
from datetime import datetime
import apache_beam as beam


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
            # Convert 'Euro_Price' to string for invalid records
            element["Euro_Price"] = str(element.get("Euro_Price", ""))
            element["Errors"] = "; ".join(errors)
            yield beam.pvalue.TaggedOutput("invalid", element)

class CheckDuplicates(beam.DoFn):
    def process(self, element):
        date, records = element
        # Yield the first record as unique
        yield records[0]
        # Yield all subsequent records as invalid
        for record in records[1:]:
            record["Errors"] = f"Duplicate record for date {date}"
            yield beam.pvalue.TaggedOutput("invalid", record)
