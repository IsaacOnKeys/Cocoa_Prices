import json
import os
import time
from datetime import datetime, timedelta, timezone

import requests
from google.cloud import pubsub_v1
from request_policy import INVOCATION_SECONDS, acquisition_identity, coverage, get, publish_batch

SCHEMA_FILE = "weather_schema.avsc"
PROJECT = os.getenv("GCP_PROJECT", "cocoa-prices-430315")
TOPIC = os.getenv("WEATHER_TOPIC", "weather-topic")

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
    Reject malformed numbers, then map explicit source missing values to None.
    Treat any value <= -999 as missing (covers -999, -999.0, sometimes -9999).
    """
    f = safe_float(x)
    if x is not None and (f is None or isinstance(x, bool)):
        raise ValueError("Invalid weather measurement")
    if f is not None and f <= SENTINEL_MISSING_MAX:
        return None
    return f


def publish_weather_data(event=None, context=None):
    deadline = time.monotonic() + INVOCATION_SECONDS
    acquisition_id = acquisition_identity("NASA_POWER", event=event, context=context)
    today = datetime.now(timezone.utc)
    progress = coverage("NASA_POWER", today.date(), deadline=deadline)
    start_date = (progress.start if progress else (today - timedelta(days=7)).date()).strftime("%Y%m%d")
    end_date = today.strftime("%Y%m%d")

    url = (
        "https://power.larc.nasa.gov/api/temporal/daily/point"
        "?parameters=PRECTOTCORR,GWETROOT"
        "&community=ag"
        "&longitude=-6.635&latitude=4.745"
        f"&start={start_date}&end={end_date}"
        "&format=JSON&time-standard=LST"
    )

    print(f"Fetching weather data from {url}")
    response = get(requests, url, deadline=deadline - 10, label="NASA POWER")

    try:
        data = response.json()
        precips = data["properties"]["parameter"]["PRECTOTCORR"]
        moistures = data["properties"]["parameter"]["GWETROOT"]
        if not isinstance(precips, dict) or not isinstance(moistures, dict):
            raise ValueError("Invalid weather series")
        if not precips or set(precips) != set(moistures):
            raise ValueError("Empty or unmatched weather series")
        if progress:
            expected = {(progress.start + timedelta(days=i)).strftime('%Y%m%d')
                        for i in range((progress.end - progress.start).days + 1)}
            if set(precips) != expected:
                raise ValueError("Incomplete weather acquisition window")
        observations = []
        for date_str in sorted(precips):
            observed = datetime.strptime(date_str, "%Y%m%d")
            if observed.strftime("%Y%m%d") != date_str or not start_date <= date_str <= end_date:
                raise ValueError("Weather observation outside requested window")
            observations.append((observed.strftime("%Y-%m-%d"),
                                 scrub_weather_val(precips[date_str]), scrub_weather_val(moistures[date_str])))
    except Exception:
        raise RuntimeError("Invalid NASA POWER response") from None

    acquired_at = datetime.now(timezone.utc).isoformat()
    schema_path = os.path.join(os.path.dirname(__file__), SCHEMA_FILE)
    with open(schema_path, "r") as f:
        schema = json.load(f)

    records = []
    for date_bq, precipitation, soil_moisture in observations:
        payload = {
            "date": date_bq,
            "precipitation": precipitation,   # None if missing
            "soil_moisture": soil_moisture,   # None if missing
            "ingestion_time": acquired_at,
            "raw_payload": json.dumps(data),
        }

        records.append(payload)
    archive = publish_batch(pubsub_v1.PublisherClient, PROJECT, TOPIC, records, schema, source='NASA_POWER', acquisition_id=acquisition_id,
                            acquired_at=acquired_at, deadline=deadline, progress=progress)

    receipt = {"source": "NASA_POWER", "status": "published", "published": len(records),
               "observation_start": observations[0][0], "observation_end": observations[-1][0],
               "acquired_at": acquired_at, "acquisition_id": acquisition_id, **archive}
    if progress:
        receipt.update(progress.complete(receipt, [{'date': d, 'precipitation': p, 'soil_moisture': m}
                                                   for d, p, m in observations]))
    print(json.dumps({"event": "acquisition_complete", **receipt}))
    return receipt


if __name__ == "__main__":
    publish_weather_data()
