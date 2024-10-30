import logging
from datetime import datetime, timedelta

import apache_beam as beam

# Configure logging if needed
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

def parse_csv(line):
    row = line.split(",")
    try:
        parsed = {
            "YEAR": int(row[0]),
            "DOY": int(row[1]),
            "precipitation": float(row[2]),
            "soil_moisture": float(row[3]),
        }
        logging.info(f"Parsed line successfully: {parsed}")
        return parsed
    except ValueError as e:
        logging.warning(f"Skipping line due to parsing error: {line}, Error: {e}")
        return None

# Clean and parse data types
def clean_and_transform(element):
    if element is None:
        return None

    # Combine YEAR and DOY to create a date
    year = element.get("YEAR")
    doy = element.get("DOY")
    try:
        date = datetime(year, 1, 1) + timedelta(days=doy - 1)
        date_str = date.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        date_str = None

    return {
        "date": date_str,
        "precipitation": element.get("precipitation"),
        "soil_moisture": element.get("soil_moisture"),
    }

# Filter out rows with missing data
def filter_missing_data(element):
    return (
        element is not None
        and element.get("precipitation") is not None
        and element.get("soil_moisture") is not None
        and element.get("date") is not None
    )

# DoFn for Validation and Transformation
class ValidateAndTransform(beam.DoFn):
    def __init__(self):
        super(ValidateAndTransform, self).__init__()
        self.start_date = datetime(2014, 1, 1)
        self.end_date = datetime(2023, 12, 31)

    def process(self, element):
        valid = True
        errors = []

        # Schema Validation
        required_fields = ["date", "precipitation", "soil_moisture"]
        if not all(field in element for field in required_fields):
            valid = False
            missing = [field for field in required_fields if field not in element]
            errors.append(f"Missing fields: {', '.join(missing)}")

        # Data Type Validation
        date_str = element.get("date")
        precipitation = element.get("precipitation")
        soil_moisture = element.get("soil_moisture")

        # Validate date format
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        except (ValueError, TypeError):
            valid = False
            errors.append("Invalid date format")
            date_obj = None

        # Validate numerical fields
        if precipitation is not None and not isinstance(precipitation, float):
            valid = False
            errors.append("precipitation is not a float")
        if soil_moisture is not None and not isinstance(soil_moisture, float):
            valid = False
            errors.append("soil_moisture is not a float")

        # Date Range Validation
        if date_obj:
            if not (self.start_date <= date_obj <= self.end_date):
                valid = False
                errors.append("Date out of range (2014-01-01 to 2023-12-31)")

        # Missing Value Handling
        if precipitation is None:
            valid = False
            errors.append("Missing precipitation value")
        if soil_moisture is None:
            valid = False
            errors.append("Missing soil_moisture value")

        if valid:
            yield {
                "date": date_str,
                "precipitation": precipitation,
                "soil_moisture": soil_moisture,
            }
        else:
            element["Errors"] = "; ".join(errors)
            yield beam.pvalue.TaggedOutput("invalid", element)

# Uniqueness checks for GroupByKey
def key_by_date(record):
    return (record["date"], record)

def filter_unique_dates(element):
    date, records = element
    records = list(records)
    if len(records) == 1:
        yield records[0]
    else:
        for record in records:
            record["Errors"] = "Duplicate date"
            yield beam.pvalue.TaggedOutput("invalid", record)

# Function to check valid records before writing to BigQuery
def check_valid_record(record):
    if (
        record["date"] is None
        or record["precipitation"] is None
        or record["soil_moisture"] is None
    ):
        raise ValueError(f"Invalid record: {record}")
    return record
