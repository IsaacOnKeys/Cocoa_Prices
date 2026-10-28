import io
import json
import os
import time
from datetime import datetime, timedelta, timezone

import fastavro
import requests
from google.cloud import pubsub_v1
from request_policy import INVOCATION_SECONDS, get, publish_timeout

SCHEMA_FILE = "weather_schema.avsc"
PROJECT = os.getenv("GCP_PROJECT", "cocoa-prices-430315")
TOPIC = "weather-topic"

# NASA POWER sometimes uses -999 (or <= -999) for "missing"
SENTINEL_MISSING_MAX = -999.0


def safe_float(x):
    """Convert to float, but return None if NaN or Infinity."""
    try:
        f = float(x)
        if f != f or f in (float("inf"), float("-inf")):
            return None
        return f
    except Exception:
        return None


def scrub_weather_val(x):
    """
    Convert input to float via safe_float, then map NASA missing sentinels to None.
    Treat any value <= -999 as missing (covers -999, -999.0, sometimes -9999).
    """
    f = safe_float(x)
    if f is not None and f <= SENTINEL_MISSING_MAX:
        return None
    return f


def publish_weather_data(event=None, context=None):
    deadline = time.monotonic() + INVOCATION_SECONDS
    today = datetime.now(timezone.utc)
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
    response = get(requests, url, deadline=deadline - 10, label="NASA POWER")

    try:
        data = response.json()
        precips = data["properties"]["parameter"]["PRECTOTCORR"]
        moistures = data["properties"]["parameter"]["GWETROOT"]
    except Exception:
        raise RuntimeError("Invalid NASA POWER response") from None

    acquired_at = datetime.now(timezone.utc).isoformat()
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT, TOPIC)

    schema_path = os.path.join(os.path.dirname(__file__), SCHEMA_FILE)
    with open(schema_path, "r") as f:
        schema = fastavro.parse_schema(json.load(f))

    published = 0
    # Use the intersection of keys in case one series is missing a date
    for date_str in sorted(set(precips.keys()) & set(moistures.keys())):
        # Scrub sentinels to None (NULL in Avro)
        precipitation = scrub_weather_val(precips[date_str])
        soil_moisture = scrub_weather_val(moistures[date_str])

        date_obj = datetime.strptime(date_str, "%Y%m%d")
        date_bq = date_obj.strftime("%Y-%m-%d")

        payload = {
            "date": date_bq,
            "precipitation": precipitation,   # None if missing
            "soil_moisture": soil_moisture,   # None if missing
            "ingestion_time": acquired_at,
            "raw_payload": json.dumps(data),
        }

        try:
            buf = io.BytesIO()
            fastavro.schemaless_writer(buf, schema, payload)
            avro_bytes = buf.getvalue()

            timeout = publish_timeout(deadline)
            future = publisher.publish(
                topic_path,
                avro_bytes,
                googclient_schemaencoding="BINARY",
            )
            future.result(timeout=min(timeout, publish_timeout(deadline)))
            print(f"[OK] Published Avro message for {date_bq} to {topic_path}")
            published += 1
        except Exception:
            raise RuntimeError("Weather Avro publication failed; batch incomplete") from None

    if published == 0:
        print(f"No weather data found for {start_date}â€“{end_date}")


if __name__ == "__main__":
    publish_weather_data()
