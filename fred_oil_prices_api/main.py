import datetime
import io
import json
import os
from datetime import timezone

import fastavro
import requests
from google.cloud import pubsub_v1, secretmanager

SCHEMA_FILE = "oil_schema.avsc"
PROJECT = os.getenv("GCP_PROJECT", "cocoa-prices-430315")
TOPIC = "oil-prices-topic"


def safe_float(x):
    """Convert to float, but return None if invalid/NaN/Infinity."""
    try:
        f = float(x)
        if f != f or f in (float("inf"), float("-inf")):
            return None
        return f
    except Exception:
        return None


def get_secret(secret_id, project_id="cocoa-prices-430315"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def publish_fred_data(event=None, context=None):
    # Get API key
    try:
        API_KEY = get_secret("FRED_OIL_API_KEY").strip()
        print("API key loaded from Secret Manager")
    except Exception:
        API_KEY = os.getenv("FRED_OIL_API_KEY", "").strip()
        print("API key loaded from environment variable")

    if not API_KEY:
        print("[FAIL] No FRED API key available")
        return

    # Build date range
    today = datetime.date.today()
    start_date = (today - datetime.timedelta(days=14)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "DCOILBRENTEU",
        "api_key": API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "observation_start": start_date,
        "observation_end": end_date,
    }

    print(f"Fetching oil data from {url} with params {params}")

    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"[FAIL] FRED API error {response.status_code}: {response.text[:200]}")
        return

    try:
        data = response.json().get("observations", [])
    except Exception as e:
        print(f"[FAIL] Failed to parse FRED API response: {e}")
        return

    if not data:
        print(f"[INFO] No oil price data found for {start_date}–{end_date}")
        return

    latest = data[0]
    oil_price = (
        safe_float(latest.get("value"))
        if latest.get("value") not in (".", None, "")
        else None
    )

    record = {
        "date": str(latest.get("date")) if latest.get("date") else None,
        "oil_price": oil_price,
        "ingestion_time": datetime.datetime.now(timezone.utc).isoformat(),
        "raw_payload": json.dumps(latest),
    }

    print("Payload prepared:", record)

    # Load Avro schema
    schema_path = os.path.join(os.path.dirname(__file__), SCHEMA_FILE)
    with open(schema_path, "r") as f:
        schema = fastavro.parse_schema(json.load(f))

    try:
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, schema, record)
        avro_bytes = buf.getvalue()

        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT, TOPIC)
        future = publisher.publish(
            topic_path,
            avro_bytes,
            googclient_schemaencoding="BINARY",
        )
        future.result()
        print(f"[OK] Published Avro message for {record['date']} to {topic_path}")
    except Exception as e:
        print(f"[FAIL] Failed to publish Avro message: {e}")


if __name__ == "__main__":
    publish_fred_data()
