import io
import json
import os
import re
from datetime import UTC, datetime

import fastavro
import functions_framework
import requests
from bs4 import BeautifulSoup, FeatureNotFound
from google.cloud import pubsub_v1

URL = "https://www.icco.org/home/"
HEADERS = {"User-Agent": "Mozilla/5.0"}
PROJECT = os.getenv("GCP_PROJECT", "cocoa-prices-430315")
TOPIC = "cocoa-prices-topic"
SCHEMA_FILE = "cocoa_schema.avsc"  # must be in the same folder as this script

def _scrape_price() -> float:
    html = requests.get(URL, headers=HEADERS, timeout=15).text
    try:
        soup = BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(html, "html.parser")

    rows = soup.select("table#table_1 tbody tr[id^=table_]")
    if not rows:
        raise RuntimeError("price rows not found")

    latest_date, latest_price = None, None
    for r in rows:
        tds = r.find_all("td")
        if len(tds) < 6:
            continue
        try:
            date_val = datetime.strptime(tds[1].text.strip(), "%d/%m/%Y").date()
        except ValueError:
            continue
        price_val = float(re.sub(",", "", tds[-1].text.strip()))
        if latest_date is None or date_val > latest_date:
            latest_date, latest_price = date_val, price_val

    if latest_price is None:
        raise RuntimeError("no valid price found")

    return latest_price

@functions_framework.http
def publish_price(request):
    cocoa_price = _scrape_price()
    now = datetime.now(UTC)

    # Build message in Avro schema format
    msg = {
        "date": now.date().isoformat(),
        "cocoa_price": cocoa_price,
        "ingestion_time": now.isoformat(),
        "raw_payload": json.dumps(
            {"date": now.date().isoformat(), "price": cocoa_price},
            separators=(",", ":"),
        ),
    }

    # Load Avro schema from file in function directory
    schema_path = os.path.join(os.path.dirname(__file__), SCHEMA_FILE)
    with open(schema_path, "r") as f:
        schema = fastavro.parse_schema(json.load(f))

    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, msg)
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


    return (
        json.dumps({"date": now.date().isoformat(), "price": cocoa_price}),
        200,
        {"Content-Type": "application/json"},
    )

if __name__ == "__main__":
    publish_price(None)
