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


def publish_weather_data(event=None, context=None):
    today = datetime.utcnow()
    start_date = (today - timedelta(days=3)).strftime("%Y%m%d")
    end_date = today.strftime("%Y%m%d")

    url = (
        "https://power.larc.nasa.gov/api/temporal/daily/point"
        "?parameters=PRECTOTCORR,GWETROOT"
        "&community=ag"
        "&longitude=-6.635&latitude=4.745"
        f"&start={start_date}&end={end_date}"
        "&format=JSON"
    )

    response = requests.get(url)
    data = response.json()

    precips = data["properties"]["parameter"]["PRECTOTCORR"]
    moistures = data["properties"]["parameter"]["GWETROOT"]

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT, TOPIC)

    schema_path = os.path.join(os.path.dirname(__file__), SCHEMA_FILE)
    with open(schema_path, "r") as f:
        schema = fastavro.parse_schema(json.load(f))

    for date_str in precips.keys():
        precipitation = precips[date_str]
        soil_moisture = moistures[date_str]

        date_obj = datetime.strptime(date_str, "%Y%m%d")
        date_bq = date_obj.strftime("%Y-%m-%d")

        payload = {
            "date": date_bq,
            "precipitation": float(precipitation),
            "soil_moisture": float(soil_moisture),
            "ingestion_time": datetime.utcnow().isoformat(),
            "raw_payload": json.dumps(data),
        }

        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, schema, payload)
        avro_bytes = buf.getvalue()

        future = publisher.publish(
            topic_path,
            avro_bytes,
            googclient_schemaencoding="BINARY",
        )
        future.result()
        print(f"Published Avro message for {date_bq} to {topic_path}")


if __name__ == "__main__":
    publish_weather_data()
