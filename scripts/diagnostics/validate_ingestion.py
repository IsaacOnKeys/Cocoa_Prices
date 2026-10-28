"""Guarded isolated integration check for the bounded daily consumer; never uses production subscriptions."""

import argparse
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import fastavro

ROOT = Path(__file__).resolve().parents[2]
PROJECT = "cocoa-prices-430315"
RUNTIME = f"analytics-staging@{PROJECT}.iam.gserviceaccount.com"
PUSH = f"analytics-push@{PROJECT}.iam.gserviceaccount.com"
TABLES = {"cocoa": "cocoa_prices", "oil": "oil_prices", "weather": "precipitation_moisture"}
MEASUREMENTS = {"cocoa": {"cocoa_price": 6500.25}, "oil": {"oil_price": 72.5},
                "weather": {"precipitation": 3.5, "soil_moisture": 0.42}}


def credentials():
    from google.oauth2.credentials import Credentials

    result = subprocess.run(
        [shutil.which("gcloud.cmd") or shutil.which("gcloud"), "auth", "print-access-token", "--verbosity=error"],
        capture_output=True, text=True, timeout=30,
        env={**os.environ, "CLOUDSDK_CORE_DISABLE_FILE_LOGGING": "true"},
    )
    if result.returncode or not result.stdout.strip():
        raise RuntimeError("Cloud authentication failed; output suppressed")
    return Credentials(result.stdout.strip())


def checked_state(path):
    state = json.loads(path.read_text())
    token = state["token"]
    if not re.fullmatch(r"[0-9]{8}_[0-9a-f]{8}", token):
        raise ValueError("Invalid validation namespace")
    if state["dataset"] != f"{PROJECT}.ingestion_validation_{token}":
        raise ValueError("Only isolated validation datasets are allowed")
    expected = {feed: f"projects/{PROJECT}/subscriptions/ingestion-validation-{token.replace('_', '-')}-{feed}" for feed in TABLES}
    if state["subscriptions"] != expected:
        raise ValueError("Only isolated validation subscriptions are allowed")
    return state


def check_routes(configured, state):
    expected = {subscription: {"feed": feed, "table": f"{state['dataset']}.{TABLES[feed]}"}
                for feed, subscription in state["subscriptions"].items()}
    if configured != expected:
        raise ValueError("Validation routes must target only the expected isolated tables")


def read_logs(credential, delivery):
    from google.auth.transport.requests import AuthorizedSession

    if not re.fullmatch(r"[0-9a-f]{64}", delivery):
        raise ValueError("Invalid delivery identity")
    result = AuthorizedSession(credential).post("https://logging.googleapis.com/v2/entries:list", json={
        "resourceNames": [f"projects/{PROJECT}"], "pageSize": 100,
        "filter": f'resource.type="cloud_run_revision" AND resource.labels.service_name="stage-analytics-data" AND jsonPayload.delivery_id="{delivery}"',
        "orderBy": "timestamp desc",
    }, timeout=30)
    result.raise_for_status()
    allowed = {"event", "feed", "delivery_id", "duration_ms", "instance_age_ms", "peak_rss_kib", "error_type"}
    return [{"timestamp": e.get("timestamp"), "revision": e.get("resource", {}).get("labels", {}).get("revision_name"),
             **{k: v for k, v in e.get("jsonPayload", {}).items() if k in allowed}}
            for e in result.json().get("entries", [])]


def prepare(path, credential):
    from google.cloud import bigquery, pubsub_v1

    if path.exists():
        raise ValueError("Refusing to replace existing validation receipt")
    token = datetime.now(timezone.utc).strftime("%Y%m%d") + "_" + uuid.uuid4().hex[:8]
    subscriptions = {feed: f"projects/{PROJECT}/subscriptions/ingestion-validation-{token.replace('_', '-')}-{feed}" for feed in TABLES}
    state = {"token": token, "dataset": f"{PROJECT}.ingestion_validation_{token}", "subscriptions": subscriptions}
    # Persist names before creation so a partial setup can be cleaned safely.
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2))
    bq = bigquery.Client(project=PROJECT, credentials=credential, location="europe-west3")
    dataset = bigquery.Dataset(state["dataset"])
    dataset.location = "europe-west3"
    dataset.default_table_expiration_ms = 86_400_000
    dataset.labels = {"app": "cocoa-prices", "purpose": "ingestion-validation"}
    bq.create_dataset(dataset)
    publisher = pubsub_v1.PublisherClient(credentials=credential)
    for feed, table in TABLES.items():
        fields = [bigquery.SchemaField("date", "DATE")]
        fields += [bigquery.SchemaField(name, "FLOAT64") for name in MEASUREMENTS[feed]]
        fields += [bigquery.SchemaField("ingestion_time", "TIMESTAMP"), bigquery.SchemaField("raw_payload", "STRING")]
        bq.create_table(bigquery.Table(f"{state['dataset']}.{table}", schema=fields))
        publisher.create_topic(request={
            "name": subscriptions[feed].replace("/subscriptions/", "/topics/"),
            "schema_settings": {"schema": f"projects/{PROJECT}/schemas/{feed}-schema", "encoding": "BINARY"},
        })
    routes = {subscription: {"feed": feed, "table": f"{state['dataset']}.{TABLES[feed]}"} for feed, subscription in subscriptions.items()}
    path.with_suffix(".tfvars.json").write_text(json.dumps({"ingestion_validation_routes": routes}, indent=2))
    print(json.dumps({"prepared": True, "dataset": state["dataset"], "routes_file": str(path.with_suffix('.tfvars.json'))}))


def exercise(path, credential):
    from google.auth.transport.requests import AuthorizedSession
    from google.cloud import bigquery, pubsub_v1

    state = checked_state(path)
    http = AuthorizedSession(credential)
    response = http.get(f"https://cloudfunctions.googleapis.com/v2/projects/{PROJECT}/locations/europe-west3/functions/stage_analytics_data", timeout=30)
    response.raise_for_status()
    function = response.json()
    assert function["state"] == "ACTIVE"
    configured = json.loads(function["serviceConfig"]["environmentVariables"]["INGESTION_ROUTES"])
    check_routes(configured, state)
    url = function["serviceConfig"]["uri"]
    vm = http.get(f"https://compute.googleapis.com/compute/v1/projects/{PROJECT}/zones/europe-west3-c/instances/airflow-vm", timeout=30)
    vm.raise_for_status()
    assert vm.json()["status"] == "TERMINATED", "Exercise requires the orchestration VM stopped"
    import requests

    assert requests.post(url, json={}, timeout=30).status_code in (401, 403), "Unauthenticated invocation must be denied"
    subscriber = pubsub_v1.SubscriberClient(credentials=credential)
    publisher = pubsub_v1.PublisherClient(credentials=credential)
    bq = bigquery.Client(project=PROJECT, credentials=credential, location="europe-west3")
    for subscription in state["subscriptions"].values():
        subscriber.create_subscription(request={
            "name": subscription, "topic": subscription.replace("/subscriptions/", "/topics/"),
            "ack_deadline_seconds": 60, "message_retention_duration": {"seconds": 86400},
            "expiration_policy": {"ttl": {"seconds": 86400}},
            "retry_policy": {"minimum_backoff": {"seconds": 10}, "maximum_backoff": {"seconds": 30}},
            "push_config": {"push_endpoint": url, "oidc_token": {"service_account_email": PUSH, "audience": url}},
        })

    def publish(feed, missing=False):
        record = {"date": "2026-10-21" if missing else "2026-10-20", "ingestion_time": "2026-10-27T10:00:00Z",
                  "raw_payload": json.dumps({"source": "isolated_fixture", "validation": state["token"]}),
                  **{name: None if missing else value for name, value in MEASUREMENTS[feed].items()}}
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, json.loads((ROOT / "schemas" / f"{feed}_schema.avsc").read_bytes()), record)
        topic = state["subscriptions"][feed].replace("/subscriptions/", "/topics/")
        return publisher.publish(topic, buffer.getvalue(), googclient_schemaencoding="BINARY").result(timeout=30)

    # A valid message must retry while its identity lacks sink access, then
    # recover after that access is granted. There are no production writes.
    message_id = publish("oil")
    delivery = hashlib.sha256(f"{state['subscriptions']['oil']}\0{message_id}".encode()).hexdigest()
    state["initial_message_id"] = message_id
    state["initial_delivery_id"] = delivery
    path.write_text(json.dumps(state, indent=2))

    deadline = time.monotonic() + 180
    failed = []
    while time.monotonic() < deadline:
        failed = read_logs(credential, delivery)
        if any(e.get("event") == "ingestion_retry" and e.get("error_type") == "HTTPError" for e in failed):
            break
        time.sleep(5)
    else:
        raise RuntimeError("Expected sink-denial retry not observed; inspect isolated route")
    assert next(iter(bq.query(f"SELECT COUNT(*) n FROM `{state['dataset']}.oil_prices`").result()))["n"] == 0
    state.update(cold_failure_logs=failed, initial_sink_rows=0, production_writes=0,
                 vm_stopped=True, unauthenticated_denied=True)
    path.write_text(json.dumps(state, indent=2))
    dataset = bq.get_dataset(state["dataset"])
    dataset.access_entries = [*dataset.access_entries, bigquery.AccessEntry("WRITER", "userByEmail", RUNTIME)]
    bq.update_dataset(dataset, ["access_entries"])
    for feed in TABLES:
        if feed != "oil":
            publish(feed)
        publish(feed, missing=True)
    verify(path, credential)


def verify(path, credential):
    """Resume read-only verification when permission/log propagation outlasts a check."""
    from google.cloud import bigquery

    state = checked_state(path)
    if not (state.get("vm_stopped") and state.get("unauthenticated_denied") and state.get("initial_sink_rows") == 0):
        raise ValueError("Resume requires the exercise's saved authentication, VM and initial-sink evidence")
    bq = bigquery.Client(project=PROJECT, credentials=credential, location="europe-west3")
    deadline = time.monotonic() + 600
    accepted = {}
    while time.monotonic() < deadline:
        accepted = {feed: list(bq.query(f"SELECT * FROM `{state['dataset']}.{table}` ORDER BY date").result()) for feed, table in TABLES.items()}
        if all({str(r['date']) for r in rows} == {'2026-10-20', '2026-10-21'} for rows in accepted.values()):
            break
        time.sleep(5)
    else:
        raise RuntimeError("Expected isolated records did not all reach staging")
    for feed, rows in accepted.items():
        for row in rows:
            assert json.loads(row["raw_payload"])["validation"] == state["token"]
            for name, value in MEASUREMENTS[feed].items():
                assert row[name] == (None if str(row['date']) == '2026-10-21' else value)
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        accepted_logs = read_logs(credential, state["initial_delivery_id"])
        if any(e.get("event") == "ingestion_accepted" for e in accepted_logs):
            break
        time.sleep(5)
    else:
        raise RuntimeError("Retried-message acceptance log not yet available; resume verify")
    assert any(e.get("event") == "ingestion_retry" and e.get("error_type") == "HTTPError" for e in accepted_logs)
    state.update(passed=True, production_writes=0,
                 sink_denial_recovered=True, rows_by_feed={feed: len(rows) for feed, rows in accepted.items()},
                 initial_message_logs=accepted_logs)
    path.write_text(json.dumps(state, indent=2))
    print(json.dumps({k: state[k] for k in ("passed", "production_writes", "vm_stopped", "unauthenticated_denied", "sink_denial_recovered", "rows_by_feed")}))


def cleanup(path, credential):
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery, pubsub_v1

    state = checked_state(path)
    subscriber = pubsub_v1.SubscriberClient(credentials=credential)
    publisher = pubsub_v1.PublisherClient(credentials=credential)
    for subscription in state["subscriptions"].values():
        try:
            subscriber.delete_subscription(request={"subscription": subscription})
        except NotFound:
            pass
        try:
            publisher.delete_topic(request={"topic": subscription.replace("/subscriptions/", "/topics/")})
        except NotFound:
            pass
    bigquery.Client(project=PROJECT, credentials=credential).delete_dataset(state["dataset"], delete_contents=True, not_found_ok=True)
    state["isolated_resources_deleted"] = True
    path.write_text(json.dumps(state, indent=2))
    print("Isolated validation resources deleted; production untouched.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "exercise", "verify", "cleanup"))
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    {"prepare": prepare, "exercise": exercise, "verify": verify, "cleanup": cleanup}[args.action](args.state, credentials())


if __name__ == "__main__":
    main()
