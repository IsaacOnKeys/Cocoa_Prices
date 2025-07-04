import io
import json
import os
from datetime import datetime, timezone

import fastavro
import requests
from dotenv import load_dotenv
from google.cloud import pubsub_v1, secretmanager

SCHEMA_FILE = "oil_schema.avsc"
PROJECT = os.getenv("GCP_PROJECT", "cocoa-prices-430315")
TOPIC = "oil-prices-topic"

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def get_secret(secret_id, project_id="cocoa-prices-430315"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def publish_fred_data(event=None, context=None):
    try:
        API_KEY = get_secret("FRED_OIL_API_KEY")
    except Exception:
        API_KEY = os.getenv("FRED_OIL_API_KEY")

    today = datetime.date.today()
    start_date = (today - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
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

    response = requests.get(url, params=params)
    data = response.json().get("observations", [{}])[0]

    record = {
        "date": str(data.get("date")) if data.get("date") else None,
        "oil_price": (
            float(data.get("value"))
            if data.get("value") not in (".", None, "")
            else None
        ),
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
        "raw_payload": json.dumps(data) if data else None,
    }

    schema_path = os.path.join(os.path.dirname(__file__), SCHEMA_FILE)
    with open(schema_path, "r") as f:
        schema = fastavro.parse_schema(json.load(f))

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
    print(f"Published Avro message to {topic_path}")


if __name__ == "__main__":
    publish_fred_data()
