import io
import json
import os
from datetime import datetime

import fastavro
import requests
from google.cloud import pubsub_v1
 
SCHEMA_FILE = "weather_schema.avsc"
PROJECT = os.getenv("GCP_PROJECT", "cocoa-prices-430315")
TOPIC = "weather-topic"


def publish_weather_data(event=None, context=None):
    today = datetime.utcnow().strftime("%Y%m%d")
    url = (
        "https://power.larc.nasa.gov/api/temporal/daily/point"
        "?parameters=PRECTOTCORR,GWETROOT"
        "&community=ag"
        "&longitude=-6.635&latitude=4.745"
        f"&start={today}&end={today}"
        "&format=JSON"
    )

    response = requests.get(url)
    data = response.json()

    date_str, precipitation = list(
        data["properties"]["parameter"]["PRECTOTCORR"].items()
    )[0]
    soil_moisture = data["properties"]["parameter"]["GWETROOT"][date_str]

    # Fix: Convert date string to BigQuery format
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    date_bq = date_obj.strftime("%Y-%m-%d")

    payload = {
        "date": date_bq,
        "precipitation": float(precipitation),
        "soil_moisture": float(soil_moisture),
        "ingestion_time": datetime.utcnow().isoformat(),
        "raw_payload": json.dumps(data),
    }

    schema_path = os.path.join(os.path.dirname(__file__), SCHEMA_FILE)
    with open(schema_path, "r") as f:
        schema = fastavro.parse_schema(json.load(f))

    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, payload)
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
    publish_weather_data()
