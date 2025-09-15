import io
import json
import os
from datetime import datetime, timedelta

import fastavro
import requests
from google.cloud import pubsub_v1

SCHEMA_FILE = "weather_schema.avsc"
PROJECT = os.getenv("GCP_PROJECT", "cocoa-prices-430315")
TOPIC = "weather-topic"


def safe_float(x):
    """Convert to float, but return None if NaN or Infinity."""
    try:
        f = float(x)
        if f != f or f in (float("inf"), float("-inf")):
            return None
        return f
    except Exception:
        return None


def publish_weather_data(event=None, context=None):
    today = datetime.utcnow()
    start_date = (today - timedelta(days=7)).strftime("%Y%m%d")
    end_date = today.strftime("%Y%m%d")

    url = (
        "https://power.larc.nasa.gov/api/temporal/daily/point"
        "?parameters=PRECTOTCORR,GWETROOT"
        "&community=ag"
        "&longitude=-6.635&latitude=4.745"
        f"&start={start_date}&end={end_date}"
        "&format=JSON"
    )

    print(f"Fetching weather data from {url}")
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Weather API error {response.status_code}: {response.text[:200]}")
        return

    try:
        data = response.json()
        precips = data["properties"]["parameter"]["PRECTOTCORR"]
        moistures = data["properties"]["parameter"]["GWETROOT"]
    except Exception as e:
        print(f"Failed to parse weather response: {e}, payload={response.text[:500]}")
        return

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT, TOPIC)

    schema_path = os.path.join(os.path.dirname(__file__), SCHEMA_FILE)
    with open(schema_path, "r") as f:
        schema = fastavro.parse_schema(json.load(f))

    published = 0
    for date_str in precips.keys():
        precipitation = safe_float(precips[date_str])
        soil_moisture = safe_float(moistures[date_str])

        date_obj = datetime.strptime(date_str, "%Y%m%d")
        date_bq = date_obj.strftime("%Y-%m-%d")

        payload = {
            "date": date_bq,
            "precipitation": precipitation,
            "soil_moisture": soil_moisture,
            "ingestion_time": datetime.utcnow().isoformat(),
            "raw_payload": json.dumps(data),
        }

        print("Payload prepared:", payload)

        try:
            buf = io.BytesIO()
            fastavro.schemaless_writer(buf, schema, payload)
            avro_bytes = buf.getvalue()

            future = publisher.publish(
                topic_path,
                avro_bytes,
                googclient_schemaencoding="BINARY",
            )
            future.result()
            print(f"[OK] Published Avro message for {date_bq} to {topic_path}")
            published += 1
        except Exception as e:
            print(f"[FAIL] Failed to publish for {date_bq}: {e}")

    if published == 0:
        print(f"No weather data found for {start_date}–{end_date}")


if __name__ == "__main__":
    publish_weather_data()
