"""Acquire daily Europe Brent observations directly from EIA and publish Avro."""

import io
import json
import math
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser

import fastavro
import requests
from google.cloud import pubsub_v1, secretmanager
from request_policy import INVOCATION_SECONDS, get, publish_timeout

SCHEMA_FILE = "oil_schema.avsc"
PROJECT = os.getenv("GCP_PROJECT", "cocoa-prices-430315")
TOPIC = os.getenv("OIL_TOPIC", "oil-prices-topic")
URL = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
PUBLISHED_URL = "https://www.eia.gov/dnav/pet/hist/RBRTED.htm"
SERIES = "RBRTE"
UNITS = "$/BBL"
PUBLIC_FIELDS = (
    "period", "series", "series-description", "value", "units", "duoarea",
    "area-name", "product", "product-name", "process", "process-name",
)


class PublishedTable(HTMLParser):
    """Read the publisher's dated weekday table without treating blanks as prices."""

    def __init__(self):
        super().__init__()
        self.rows, self.title = [], []
        self.row = self.cell = None
        self.in_title = False

    def handle_starttag(self, tag, attrs):
        if tag == "title":
            self.in_title = True
        elif tag == "tr":
            self.row = []
        elif tag in ("td", "th") and self.row is not None:
            self.cell = []

    def handle_data(self, data):
        if self.in_title:
            self.title.append(data)
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if tag == "title":
            self.in_title = False
        elif tag in ("td", "th") and self.cell is not None:
            self.row.append("".join(self.cell).strip())
            self.cell = None
        elif tag == "tr" and self.row is not None:
            self.rows.append(self.row)
            self.row = None


def parse_published_observations(html, start, end):
    parser = PublishedTable()
    parser.feed(html)
    title = " ".join("".join(parser.title).split())
    if "Europe Brent Spot Price FOB" not in title or "Dollars per Barrel" not in title:
        raise RuntimeError("Unexpected EIA published product or units")
    months = {name: index for index, name in enumerate(
        ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), 1
    )}
    records, weeks = {}, 0
    for cells in parser.rows:
        match = re.match(r"^(\d{4})\s+([A-Za-z]{3})-\s*(\d{1,2})\s+to\s+", cells[0] if cells else "")
        if not match:
            continue
        weeks += 1
        if len(cells) != 6:
            raise RuntimeError("Unexpected EIA published table layout")
        year, month, day = match.groups()
        week = date(int(year), months[month], int(day))
        if week.weekday() != 0:
            raise RuntimeError("Unexpected EIA published week alignment")
        for index, value in enumerate(cells[1:]):
            observed = week + timedelta(days=index)
            if not start <= observed <= end or value in ("", "-", "--", "NA", "W"):
                continue
            row = {"period": observed.isoformat(), "value": value.replace(",", ""), "series": SERIES, "units": UNITS}
            record = normalize_observation(row, start, end)
            if record["date"] in records:
                raise RuntimeError("Duplicate EIA published observation")
            records[record["date"]] = record
    if not weeks:
        raise RuntimeError("EIA published table has no dated rows")
    return [records[key] for key in sorted(records)]


def fetch_published_observations(start, end, *, deadline=None):
    deadline = time.monotonic() + 40 if deadline is None else deadline
    response = get(requests, PUBLISHED_URL, deadline=deadline, label="EIA published table")
    try:
        return parse_published_observations(response.text, start, end)
    except (ValueError, TypeError, KeyError):
        raise RuntimeError("Invalid EIA published table") from None


def verify_against_published(observations, start, end, *, deadline=None):
    published = fetch_published_observations(start, end, deadline=deadline)
    actual = {r["date"]: r["oil_price"] for r in observations if r["oil_price"] is not None}
    expected = {r["date"]: r["oil_price"] for r in published}
    if actual != expected:
        raise RuntimeError("EIA API and published observations disagree; publication withheld")
    for record in observations:
        record["published_verified"] = True


def get_secret(secret_id, project_id=None):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id or PROJECT}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name}, timeout=10, retry=None)
    return response.payload.data.decode("UTF-8")


def load_api_key():
    try:
        key = get_secret("EIA_OIL_API_KEY").strip()
    except Exception:
        # Never log SDK errors: exceptions may contain sensitive request context.
        key = os.getenv("EIA_OIL_API_KEY", "").strip()
    if not key:
        raise RuntimeError("No EIA API key available")
    return key


def normalize_observation(row, start, end):
    """Validate source identity and units before mapping to the existing wire shape."""
    if not isinstance(row, dict) or row.get("series") != SERIES:
        raise ValueError("Unexpected EIA series")
    if row.get("units") != UNITS:
        raise ValueError("Unexpected EIA units")
    period = row.get("period")
    if not isinstance(period, str):
        raise ValueError("Missing EIA observation date")
    observed = date.fromisoformat(period)
    if observed.isoformat() != period or not start <= observed <= end:
        raise ValueError("EIA observation outside requested date window")
    value = row.get("value")
    if value in (None, "", ".", "NA", "--", "-"):
        price = None
    else:
        if isinstance(value, bool):
            raise ValueError("Invalid EIA measurement type")
        price = float(value)
        if not math.isfinite(price):
            raise ValueError("Non-finite EIA measurement")
    public_row = {key: row[key] for key in PUBLIC_FIELDS if key in row}
    return {"date": period, "oil_price": price, "source_row": public_row}


def fetch_observations(api_key, start, end, *, page_size=5000, deadline=None):
    """Fetch a bounded window with explicit ordering, pagination and safe failures."""
    if not isinstance(start, date) or not isinstance(end, date) or start > end:
        raise ValueError("Invalid EIA date window")
    if type(page_size) is not int or not 1 <= page_size <= 5000:
        raise ValueError("Invalid EIA page size")
    deadline = time.monotonic() + 50 if deadline is None else deadline
    offset, expected_total = 0, None
    records = {}
    for _ in range(20):
        remaining = deadline - time.monotonic()
        if remaining <= 1:
            raise RuntimeError("EIA acquisition deadline exceeded")
        params = {
            "api_key": api_key,
            "frequency": "daily",
            "data[0]": "value",
            "facets[series][]": SERIES,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "sort[0][column]": "period",
            "sort[0][direction]": "asc",
            "offset": offset,
            "length": page_size,
        }
        response = get(requests, URL, params=params, deadline=deadline, label="EIA API")
        try:
            payload = response.json()
            if payload.get("error"):
                raise ValueError("EIA error response")
            result = payload["response"]
            rows = result["data"]
            if not str(result["total"]).isdecimal():
                raise ValueError("Invalid EIA total")
            total = int(result["total"])
            if not isinstance(rows, list) or total > (end - start).days + 1:
                raise ValueError("Invalid EIA page")
            if expected_total is None:
                expected_total = total
            if total != expected_total or offset + len(rows) > total:
                raise ValueError("EIA pagination changed during acquisition")
            if not rows and offset != total:
                raise ValueError("Incomplete EIA page")
            for row in rows:
                record = normalize_observation(row, start, end)
                if record["date"] in records:
                    raise ValueError("Duplicate EIA date in acquisition")
                records[record["date"]] = record
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
            raise RuntimeError("Invalid or incomplete EIA response") from None
        offset += len(rows)
        if offset == total:
            return [records[key] for key in sorted(records)]
    raise RuntimeError("EIA acquisition exceeded page limit")


def to_wire_record(observation, acquired_at):
    return {
        "date": observation["date"],
        "oil_price": observation["oil_price"],
        "ingestion_time": acquired_at,
        "raw_payload": json.dumps(
            {"source": "EIA", "series": SERIES, "observation": observation["source_row"],
             "published_verified": observation.get("published_verified", False)},
            sort_keys=True, separators=(",", ":"),
        ),
    }


def publish_eia_data(event=None, context=None):
    deadline = time.monotonic() + INVOCATION_SECONDS
    now = datetime.now(timezone.utc)
    end = now.date()
    observations = fetch_observations(load_api_key(), end - timedelta(days=14), end, deadline=deadline - 10)
    verify_against_published(observations, end - timedelta(days=14), end, deadline=deadline - 10)
    if not observations:
        # Source-calendar gaps are explicit; publication gates decide freshness.
        receipt = {"source": "EIA", "status": "no_observations", "published": 0,
                   "observation_start": None, "observation_end": None, "acquired_at": now.isoformat()}
        print(json.dumps({"event": "acquisition_complete", **receipt}))
        return receipt
    with open(os.path.join(os.path.dirname(__file__), SCHEMA_FILE), encoding="utf-8") as handle:
        schema = fastavro.parse_schema(json.load(handle))
    try:
        publisher = pubsub_v1.PublisherClient()
        topic = publisher.topic_path(PROJECT, TOPIC)
        for observation in observations:
            record = to_wire_record(observation, now.isoformat())
            buffer = io.BytesIO()
            fastavro.schemaless_writer(buffer, schema, record)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("EIA publication deadline exceeded")
            publisher.publish(topic, buffer.getvalue(), googclient_schemaencoding="BINARY").result(timeout=publish_timeout(deadline))
    except Exception:
        # Partial batches must fail; retries need downstream key deduplication.
        raise RuntimeError("EIA Avro publication failed") from None
    receipt = {"source": "EIA", "status": "published", "published": len(observations),
               "observation_start": observations[0]["date"], "observation_end": observations[-1]["date"],
               "acquired_at": now.isoformat()}
    print(json.dumps({"event": "acquisition_complete", **receipt}))
    return receipt


if __name__ == "__main__":
    publish_eia_data()
