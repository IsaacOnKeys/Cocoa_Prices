import json
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

# Step 1: Function to extract and transform the data (renaming and cleaning)
def extract_and_clean(file_content):
    # Parse the JSON string
    data = json.loads(file_content)
    
    # Extract observations
    observations = data.get("observations", [])
    
    for obs in observations:
        date = obs.get("date")
        value = obs.get("value")
        
        # Clean: replace invalid values (like ".") with None
        if value == ".":
            value = None
        else:
            try:
                value = float(value)
            except ValueError:
                value = None  # Will be handled in validation
        
        # Yield the cleaned row (date, brent_price_eu)
        yield {
            "date": date,
            "brent_price_eu": value
        }

# Step 2: Validation and Conversion DoFn
class ValidateAndConvert(beam.DoFn):
    def __init__(self):
        super(ValidateAndConvert, self).__init__()
        # Define plausible range for Brent prices (Removed as per request)
        # self.min_price = 50.0
        # self.max_price = 150.0
        # Define date range
        self.start_date = datetime(2014, 1, 1)
        self.end_date = datetime(2023, 12, 31)
    
    def process(self, element):
        valid = True
        errors = []
        result = {}
        
        # Schema Validation: Check presence of 'date' and 'brent_price_eu'
        if 'date' not in element or 'brent_price_eu' not in element:
            valid = False
            errors.append("Missing 'date' or 'brent_price_eu' field")
        
        # Data Type Validation and Date Range Validation
        try:
            date_obj = datetime.strptime(element['date'], "%Y-%m-%d")
            result['date'] = date_obj
            # Date Range Validation
            if not (self.start_date <= date_obj <= self.end_date):
                valid = False
                errors.append("Date out of range")
        except (ValueError, TypeError):
            valid = False
            errors.append("Invalid date format")
        
        # Missing Value Handling and Data Type Validation for 'brent_price_eu'
        brent_price = element.get('brent_price_eu')
        if brent_price is None:
            valid = False
            errors.append("Missing Brent_Price")
        else:
            if not isinstance(brent_price, float):
                valid = False
                errors.append("Brent_Price is not a float")
            # Value Range Checks removed
        
        # Uniqueness Checks
        # Note: Using instance variables like sets in DoFns is not reliable in distributed environments.
        # Instead, uniqueness should be handled using Beam transforms like GroupByKey.
        # For simplicity, this example does not implement distributed uniqueness checks.
        # You may need to implement a more robust method for uniqueness in production.
        
        if valid:
            yield {
                "date": result['date'].strftime("%Y-%m-%d"),
                "brent_price_eu": brent_price
            }
        else:
            element['Errors'] = "; ".join(errors)
            yield beam.pvalue.TaggedOutput('invalid', element)

# Step 3: Format the data to CSV format
def format_to_csv(element):
    date = element['date']
    brent_price_eu = element['brent_price_eu']
    
    # Handle the case where brent_price_eu is None
    if brent_price_eu is None:
        brent_price_eu = ''
    
    # Create the CSV line
    return f"{date},{brent_price_eu}"

# Function to format invalid records as CSV
def format_invalid_to_csv(element):
    date = element.get('date', '')
    brent_price_eu = element.get('brent_price_eu', '')
    errors = element.get('Errors', '')
    
    return f"{date},{brent_price_eu},{errors}"

# Step 4: Define the pipeline
def run():
    pipeline_options = PipelineOptions()
    options = pipeline_options.view_as(StandardOptions)
    options.runner = "DirectRunner"  # Use 'DataflowRunner' for cloud execution

    with beam.Pipeline(options=pipeline_options) as p:
        validated_records = (
            p
            | 'Read JSON' >> beam.io.ReadFromText('RAW/brent_oil_fred.json')
            | 'Combine Lines' >> beam.CombineGlobally(lambda lines: '\n'.join(lines)).without_defaults()
            | 'Extract and Clean' >> beam.FlatMap(extract_and_clean)
            | 'Validate and Convert' >> beam.ParDo(ValidateAndConvert()).with_outputs('invalid', main='valid')
        )

        # Directly access the valid and invalid PCollections
        valid_records = validated_records.valid
        invalid_records = validated_records.invalid

        # Write valid records to CSV
        (
            valid_records
            | "Format Valid to CSV" >> beam.Map(format_to_csv)
            | "Write Valid CSV" >> beam.io.WriteToText(
                'TEST/brent_prices_cleaned',
                file_name_suffix='.csv',
                header='date,brent_price_eu'
            )
        )

        # Write invalid records to a separate CSV
        (
            invalid_records
            | "Format Invalid to CSV" >> beam.Map(format_invalid_to_csv)
            | "Write Invalid CSV" >> beam.io.WriteToText(
                'TEST/brent_prices_invalid',
                file_name_suffix='.csv',
                header='date,brent_price_eu,Errors'
            )
        )

if __name__ == "__main__":
    run()
