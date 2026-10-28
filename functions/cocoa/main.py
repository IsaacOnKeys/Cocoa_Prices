import io
import json
import os
import re
import time
from datetime import UTC, date, datetime

import fastavro
import functions_framework
import requests
from bs4 import BeautifulSoup, FeatureNotFound
from google.cloud import pubsub_v1
from request_policy import INVOCATION_SECONDS, get, publish_timeout

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


def _scrape_price(*, deadline=None) -> tuple[date, float]:
    print(f"Fetching ICCO cocoa price from {URL}")
    deadline = time.monotonic() + 40 if deadline is None else deadline
    html = get(requests, URL, headers=HEADERS, deadline=deadline, label="ICCO").text
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
    return latest_date, latest_price


@functions_framework.http
def publish_price(request):
    deadline = time.monotonic() + INVOCATION_SECONDS
    try:
        observation_date, cocoa_price = _scrape_price(deadline=deadline - 10)
    except Exception:
        print("[FAIL] ICCO acquisition failed")
        return (
            json.dumps({"error": "ICCO acquisition failed"}),
            500,
            {"Content-Type": "application/json"},
        )

    now = datetime.now(UTC)

    # Build message in Avro schema format
    payload = {
        "date": observation_date.isoformat(),
        "cocoa_price": cocoa_price,
        "ingestion_time": now.isoformat(),
        "raw_payload": json.dumps(
            {"date": observation_date.isoformat(), "price": cocoa_price,
             "date_basis": "source_observation"},
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
        timeout = publish_timeout(deadline)
        future = publisher.publish(
            topic_path,
            avro_bytes,
            googclient_schemaencoding="BINARY",
        )
        future.result(timeout=min(timeout, publish_timeout(deadline)))
        print(f"[OK] Published Avro message for {payload['date']} to {topic_path}")
    except Exception:
        print("[FAIL] Cocoa Avro publication failed")
        return (
            json.dumps({"error": "Cocoa Avro publication failed"}),
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
