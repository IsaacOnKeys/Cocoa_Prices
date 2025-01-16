FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir apache-beam[gcp]==2.61.0

WORKDIR /app

COPY COPY clean_precipitation.py clean_oil_fred.py clean_cocoa_prices.py /app/

ENTRYPOINT ["python"] 