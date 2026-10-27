"""Exercise the deployed EIA trigger through the actual oil Beam job into isolated BigQuery."""

import argparse
import hashlib
import json
import os
import re
import runpy
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROJECT = "cocoa-prices-430315"
SUBSCRIPTION = f"projects/{PROJECT}/subscriptions/eia-oil-validation-sub"


def credentials():
    from google.oauth2.credentials import Credentials

    cli = shutil.which("gcloud.cmd") or shutil.which("gcloud")
    result = subprocess.run(
        [cli, "auth", "print-access-token", "--verbosity=error", "--quiet"],
        capture_output=True, text=True, timeout=30,
        env={**os.environ, "CLOUDSDK_CORE_DISABLE_FILE_LOGGING": "true"},
    )
    if result.returncode or not result.stdout.strip():
        raise RuntimeError("Cloud authentication failed; CLI output suppressed")
    return Credentials(result.stdout.strip())


def check_table(table):
    if not re.fullmatch(r"cocoa-prices-430315\.cocoa_eia_validation_[a-z0-9_]+\.oil_prices", table):
        raise ValueError("Only the isolated EIA validation table namespace is allowed")


def consume(table):
    import google.auth

    check_table(table)
    credential = credentials()
    google.auth.default = lambda *args, **kwargs: (credential, PROJECT)
    os.environ.update({
        "GOOGLE_CLOUD_PROJECT": PROJECT,
        "OIL_SUBSCRIPTION": SUBSCRIPTION,
        "OIL_BQ_TABLE": table.replace(".", ":", 1),
        "OIL_AVRO_SCHEMA_PATH": str(ROOT / "schemas/oil_schema.avsc"),
    })
    runpy.run_path(str(ROOT / "pipelines/streaming/oil_stream.py"), run_name="__main__")


def validate(archive_path, output):
    from google.cloud import bigquery, pubsub_v1

    archive = json.loads(archive_path.read_text(encoding="utf-8"))
    if archive.get("acquisition_interface") != "published-table" or archive.get("series") != "RBRTE":
        raise ValueError("Expected the accepted EIA published-table archive")
    today = datetime.now(timezone.utc).date()
    start = (today - timedelta(days=14)).isoformat()
    expected = {
        row["period"]: float(row["value"]) for row in archive["response"]["data"]
        if start <= row["period"] <= today.isoformat() and row.get("value") is not None
    }
    if not expected:
        raise ValueError("Expected current-window prices are absent from the archive")
    credential = credentials()
    client = bigquery.Client(project=PROJECT, credentials=credential, location="europe-west3")
    dataset_id = f"{PROJECT}.cocoa_eia_validation_{today.strftime('%Y%m%d')}_{uuid.uuid4().hex[:8]}"
    table = dataset_id + ".oil_prices"
    check_table(table)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "europe-west3"
    dataset.default_table_expiration_ms = 86_400_000
    dataset.labels = {"app": "cocoa-prices", "purpose": "eia-validation"}
    client.create_dataset(dataset)
    child, success, records = None, False, []
    output.mkdir(parents=True, exist_ok=True)
    report = {"table": table, "subscription": SUBSCRIPTION, "production_writes": 0}
    try:
        schema = [bigquery.SchemaField(name, kind) for name, kind in (
            ("date", "DATE"), ("oil_price", "FLOAT64"),
            ("ingestion_time", "TIMESTAMP"), ("raw_payload", "STRING"),
        )]
        client.create_table(bigquery.Table(table, schema=schema))
        with (output / "beam-shadow.log").open("wb") as log:
            child = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "--consume", table],
                cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "CLOUDSDK_CORE_DISABLE_FILE_LOGGING": "true"},
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
            publisher = pubsub_v1.PublisherClient(credentials=credential)
            report["trigger_message_id"] = publisher.publish(
                f"projects/{PROJECT}/topics/eia-oil-trigger", b"{}"
            ).result(timeout=15)
            print("Isolated EIA trigger dispatched; waiting for actual Beam/BigQuery output.", flush=True)
            deadline = time.monotonic() + 150
            while time.monotonic() < deadline:
                if child.poll() is not None:
                    raise RuntimeError("Actual oil consumer exited before validation completed")
                records = list(client.query(
                    f"SELECT date, oil_price, ingestion_time, raw_payload FROM `{table}`"
                ).result(timeout=20))
                actual = {row.date.isoformat(): row.oil_price for row in records}
                if actual == expected:
                    for row in records:
                        payload = json.loads(row.raw_payload)
                        if payload.get("source") != "EIA" or payload.get("published_verified") is not True:
                            raise RuntimeError("Uncorroborated or wrong-source record reached the sink")
                        if row.ingestion_time is None:
                            raise RuntimeError("Missing receipt timestamp at sink")
                    success = True
                    break
                if any(day not in expected or expected[day] != price for day, price in actual.items()):
                    raise RuntimeError("Unexpected dates or prices in isolated output")
                time.sleep(3)
            if not success:
                raise RuntimeError("Timed out waiting for complete isolated output")
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=10)
        report.update({
            "passed": success, "staging_rows": len(records), "unique_expected_dates": len(expected),
            "schema_sha256": hashlib.sha256((ROOT / "schemas/oil_schema.avsc").read_bytes()).hexdigest(),
            "validation_dataset_deleted": False,
        })
        if success:
            # Delete only the unique dataset created by this run after checking its namespace.
            check_table(table)
            client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
            report["validation_dataset_deleted"] = True
        (output / "cloud-route-validation.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consume", help=argparse.SUPPRESS)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--output", type=Path, default=ROOT / ".build/eia-migration")
    args = parser.parse_args()
    if args.consume:
        consume(args.consume)
    elif args.archive:
        validate(args.archive, args.output)
    else:
        parser.error("--archive is required")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit(f"EIA isolated route validation failed ({type(error).__name__}); see private validation evidence") from None
