# Tracking Historical Cocoa Prices in Switzerland (2014-2023)

## Overview

This project is a comprehensive data engineering pipeline that tracks and analyzes historical cocoa prices in Switzerland from 2014 to 2023. By integrating various datasets—including cocoa prices, Brent oil prices, and weather data from Côte d'Ivoire—the project aims to uncover patterns and factors influencing cocoa price fluctuations.

## Project Objectives

- **Data Integration**: Combine multiple data sources to provide a holistic view of factors affecting cocoa prices.
- **Data Validation and Cleaning**: Ensure data quality through rigorous validation and error handling.
- **Scalability**: Utilize cloud-based technologies to process large datasets efficiently.
- **Analytics-Ready Data**: Store processed data in a format suitable for analysis and visualization.

## Technologies Used

- **Programming Language**: Python
- **Data Processing Framework**: Apache Beam
- **Cloud Platform**: Google Cloud Platform (GCP)
  - **Dataflow**: For executing Apache Beam pipelines
  - **BigQuery**: For data storage and analytics
  - **Cloud Storage**: For data staging and temporary storage
- **Logging**: Python's `logging` module for monitoring pipeline execution

## Data Sources

1. **Cocoa Prices**: Daily cocoa prices in Euros obtained from the [International Cocoa Organization](https://www.icco.org/).
2. **Brent Oil Prices**: Daily Brent crude oil prices sourced from the [Federal Reserve Economic Data (FRED)](https://fred.stlouisfed.org/).
3. **Weather Data**: Daily precipitation and soil moisture levels in Côte d'Ivoire from NASA's [POWER Project](https://power.larc.nasa.gov/).

## Pipeline Architecture

![Pipeline Architecture Diagram](images/pipeline_diagram.png)

The data processing pipeline consists of three main scripts, each handling a specific dataset:

1. **Cocoa Prices Data Pipeline**
2. **Brent Oil Prices Data Pipeline**
3. **Weather Data Pipeline**

Each pipeline performs the following steps:

- **Data Ingestion**: Reads raw data from Google Cloud Storage.
- **Parsing**: Parses the data into structured formats.
- **Validation and Transformation**: Validates data types, formats dates, and transforms fields as necessary.
- **Deduplication**: Checks for and removes duplicate records based on the date.
- **Error Handling**: Separates invalid records for further analysis.
- **Data Loading**: Writes valid data to BigQuery tables and invalid data to separate BigQuery tables.

## Scripts Overview

### 1. Cocoa Prices Data Pipeline

**Script**: `clean_cocoa_prices.py`

This script processes daily cocoa prices data.

#### Key Steps:

- **Parsing CSV Data**: Reads the CSV file and extracts the date and Euro price.
- **Date Transformation**: Converts dates from `dd/mm/yyyy` to `yyyy-mm-dd` format.
- **Price Validation**: Ensures the Euro price is a positive float.
- **Data Validation**: Checks for invalid dates and prices, tagging invalid records.
- **Data Loading**: Writes valid records to `cocoa_prices.cocoa` table and invalid records to `cocoa_prices.invalid_cocoa` table in BigQuery.

#### Sample Code Snippet:

```python
def parse_csv(line):
    reader = csv.reader([line])
    row = next(reader)
    return {
        "Date": row[0].strip('"'),
        "Euro_Price": row[4].strip('"'),
    }
