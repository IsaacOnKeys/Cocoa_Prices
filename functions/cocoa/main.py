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


def safe_float(x):
    """Convert to float, but return None if NaN or Infinity."""
    try:
        f = float(x)
        if f != f or f in (float("inf"), float("-inf")):
            return None
        return f
    except Exception:
        return None


def _scrape_price() -> float:
    print(f"Fetching ICCO cocoa price from {URL}")
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
        try:
            price_val = safe_float(re.sub(",", "", tds[-1].text.strip()))
        except Exception:
            continue
        if latest_date is None or date_val > latest_date:
            latest_date, latest_price = date_val, price_val

    if latest_price is None:
        raise RuntimeError("no valid price found")

    print(f"Latest cocoa price: {latest_price} on {latest_date}")
    return latest_price


@functions_framework.http
def publish_price(request):
    try:
        cocoa_price = _scrape_price()
    except Exception as e:
        print(f"[FAIL] Scraping error: {e}")
        return (
            json.dumps({"error": str(e)}),
            500,
            {"Content-Type": "application/json"},
        )

    now = datetime.now(UTC)

    # Build message in Avro schema format
    payload = {
        "date": now.date().isoformat(),
        "cocoa_price": cocoa_price,
        "ingestion_time": now.isoformat(),
        "raw_payload": json.dumps(
            {"date": now.date().isoformat(), "price": cocoa_price},
            separators=(",", ":"),
        ),
    }

    print("Payload prepared:", payload)

    # Load Avro schema from file in function directory
    schema_path = os.path.join(os.path.dirname(__file__), SCHEMA_FILE)
    with open(schema_path, "r") as f:
        schema = fastavro.parse_schema(json.load(f))

    try:
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
        print(f"[OK] Published Avro message for {payload['date']} to {topic_path}")
    except Exception as e:
        print(f"[FAIL] Failed to publish Avro message: {e}")
        return (
            json.dumps({"error": str(e)}),
            500,
            {"Content-Type": "application/json"},
        )

    return (
        json.dumps({"date": payload["date"], "price": cocoa_price}),
        200,
        {"Content-Type": "application/json"},
    )


if __name__ == "__main__":
    publish_price(None)
