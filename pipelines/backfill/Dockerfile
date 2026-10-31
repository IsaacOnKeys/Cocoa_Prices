FROM python:3.11-slim@sha256:673719e071ecf27434fd2635bbd8d00f49f4fdd81f243143438a42ced8bedf60

ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt
COPY constraints-python311.txt /constraints-python311.txt
RUN python -m pip install --no-cache-dir -r /requirements.txt -c /constraints-python311.txt \
    && python -m pip check

WORKDIR /app

COPY backfill/clean_precipitation.py backfill/clean_oil_eia.py backfill/clean_cocoa_prices.py backfill/backfill_recovery.py backfill/backfill_contract.py backfill/setup.py /app/

ENTRYPOINT ["python"]
