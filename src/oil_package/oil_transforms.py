import json
from datetime import datetime
import apache_beam as beam

def extract_and_clean(file_content):
    data = json.loads(file_content)
    observations = data.get("observations", [])
    for obs in observations:
        date = obs.get("date")
        value = obs.get("value")
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
        if "date" not in element or "brent_price_eu" not in element:
            valid = False
            errors.append("Missing 'date' or 'brent_price_eu' field")
        try:
            date_obj = datetime.strptime(element["date"], "%Y-%m-%d")
            result["date"] = date_obj
            if not (self.start_date <= date_obj <= self.end_date):
                valid = False
                errors.append("Date out of range")
        except (ValueError, TypeError):
            valid = False
            errors.append("Invalid date format")
        brent_price = element.get("brent_price_eu")
        if brent_price is None:
            valid = False
            errors.append("Missing Brent_Price")
        elif not isinstance(brent_price, float):
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


class CheckUniqueness(beam.DoFn):
    def process(self, element):
        date, records = element
        if len(records) > 1:
            for record in records:
                record["Errors"] = f"Duplicate record for date {date}"
                yield beam.pvalue.TaggedOutput("invalid", record)
        else:
            yield records[0]