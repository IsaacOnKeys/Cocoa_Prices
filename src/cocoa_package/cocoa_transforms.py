import csv
from datetime import datetime

import apache_beam as beam


def parse_csv(line):
    """
    Parses a CSV line into a dictionary with selected fields.

    Args:
        line (str): A single line of CSV data.

    Returns:
        dict: A dictionary containing:
            - "Date" (str): The date field from the CSV, with leading/trailing quotes removed.
            - "Euro_Price" (str): The price field from the CSV, with leading/trailing quotes removed.
    """
    reader = csv.reader([line])
    row = next(reader)
    return {
        "Date": row[0].strip('"'),
        "Euro_Price": row[4].strip('"'),
    }


class ValidateAndTransform(beam.DoFn):
    """
    A DoFn class for validating and transforming parsed records.

    Validation:
        - Ensures the "Date" field is in the format "dd/mm/yyyy" and converts it to "yyyy-mm-dd".
        - Ensures the "Euro_Price" field is a positive float. If invalid, stores errors in an "Errors" field.

    Yields:
        - Valid records as-is.
        - Invalid records tagged with "invalid".

    Example:
        Input: {"Date": "31/12/2023", "Euro_Price": "1,234.56"}
        Output:
            - Valid: {"Date": "2023-12-31", "Euro_Price": 1234.56}
            - Invalid: {"Date": "invalid", "Euro_Price": "", "Errors": "Invalid date format"}
    """

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
    """
    A DoFn class for identifying and handling duplicate records.

    Process:
        - For a given key (e.g., "Date"), yields the first record as unique.
        - Tags subsequent records with "invalid" and annotates them with an error message.

    Args:
        element (tuple): A tuple containing:
            - date (str): The key (e.g., "Date").
            - records (list): A list of records with the same key.

    Yields:
        - Unique records as-is.
        - Duplicate records tagged with "invalid" and annotated with an error message.

    Example:
        Input: ("2023-12-31", [{"Date": "2023-12-31", "Euro_Price": 1234.56}, {"Date": "2023-12-31", "Euro_Price": 1234.56}])
        Output:
            - Unique: {"Date": "2023-12-31", "Euro_Price": 1234.56}
            - Invalid: {"Date": "2023-12-31", "Euro_Price": 1234.56, "Errors": "Duplicate record for date 2023-12-31"}
    """

    def process(self, element):
        date, records = element
        # Yield the first record as unique
        yield records[0]
        # Yield all subsequent records as invalid
        for record in records[1:]:
            record["Errors"] = f"Duplicate record for date {date}"
            yield beam.pvalue.TaggedOutput("invalid", record)
