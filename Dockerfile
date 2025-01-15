# Use Python 3.12 as the base image
FROM python:3.12-slim

# Set environment variables for non-interactive installs and Python behavior
ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    apache-beam[gcp]==2.61.0 \
    setuptools>=75.3.0 \
    annotated-types==0.7.0

# Set the working directory in the container
WORKDIR /app

# Copy the pipeline code and requirements file into the image
COPY clean_precipitation.py /app/clean_precipitation.py
COPY clean_precipitation.py /app/clean_precipitation.py
COPY clean_cocoa_prices.py /app/clean_cocoa_prices.py
COPY clean_oil.py /app/clean_oil_three.py

# Specify the entrypoint to run the pipeline
ENTRYPOINT ["python", "clean_precipitation.py"]