import io
import json
import os
from datetime import datetime, timezone

import fastavro
import requests
from dotenv import load_dotenv
from google.cloud import pubsub_v1

SCHEMA_FILE = "oil_schema.avsc"
PROJECT = os.getenv("GCP_PROJECT", "cocoa-prices-430315")
TOPIC = "oil-prices-topic"
load_dotenv()


def publish_fred_data(event=None, context=None):

    API_KEY = os.getenv("FRED_OIL_API_KEY")
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "DCOILBRENTEU",
        "api_key": API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": "1",
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
