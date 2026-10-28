"""Isolated Gen2 retry/DLQ/replay probe; never targets production input topics."""

import argparse
import base64
import hashlib
import io
import json
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from zipfile import ZIP_STORED, ZipFile

PROJECT = "cocoa-prices-430315"
REGION = "europe-west3"
NUMBER = "494039866722"
BUCKET = "cocoa-prices-temp-for-bq"
ROOT = Path(__file__).resolve().parents[2]
ACCOUNT = f"{NUMBER}-compute@developer.gserviceaccount.com"

PROBE = '''import base64,json,os
from google.cloud import storage
from google.api_core.exceptions import NotFound
from request_policy import acquisition_identity

def delivery_probe(event,context):
    body=json.loads(base64.b64decode(event["data"],validate=True))
    scenario=body["scenario"]
    if scenario not in ("transient","permanent"):
        raise RuntimeError("Invalid probe scenario")
    identity=acquisition_identity("DELIVERY_PROBE",event=event,context=context)
    bucket=storage.Client().bucket(os.environ["PROBE_BUCKET"])
    prefix=os.environ["PROBE_PREFIX"]
    blob=bucket.blob(prefix+"attempts/"+identity+".json")
    try:
        blob.reload(timeout=10,retry=None)
        previous=json.loads(blob.download_as_bytes(if_generation_match=blob.generation,timeout=10,retry=None))
        generation=blob.generation
    except NotFound:
        previous={"attempts":0,"event_ids":[]};generation=0
    attempt=previous["attempts"]+1
    ids=previous["event_ids"]
    if context.event_id not in ids:ids.append(context.event_id)
    accepted=(scenario=="transient" and attempt>1) or (scenario=="permanent" and bucket.blob(prefix+"accept-permanent").exists(timeout=10,retry=None))
    record={"scenario":scenario,"acquisition_id":identity,"attempts":attempt,"event_ids":ids,"accepted":accepted}
    blob.upload_from_string(json.dumps(record),content_type="application/json",if_generation_match=generation,timeout=10,retry=None)
    print(json.dumps({"event":"delivery_probe",**record}))
    if not accepted:raise RuntimeError("Controlled retry probe failure")
'''


def names(token):
    if not isinstance(token, str) or not re.fullmatch(r"20[0-9]{6}_[0-9a-f]{8}", token):
        raise ValueError("Invalid delivery-validation namespace")
    function = "delivery-validation-" + token.replace("_", "-")
    return {"function": function, "topic": function + "-trigger", "dead_letter": function + "-dead-letter",
            "dead_letter_sub": function + "-dead-letter-sub", "prefix": "retry-validation/" + token + "/"}


def checked(path):
    state = json.loads(path.read_text())
    if state.get("names") != names(state["token"]):
        raise ValueError("Delivery-validation resource names were altered")
    source = state.get("source")
    if source is not None and (
        source.get("bucket") != f"gcf-v2-sources-{NUMBER}-{REGION}"
        or not re.fullmatch(r"[0-9a-f]{64}", source.get("sha256", ""))
        or source.get("object") != state["names"]["function"] + "/probe-" + source["sha256"] + ".zip"
        or not str(source.get("generation", "")).isdecimal()
    ):
        raise ValueError("Invalid probe source reference")
    return state


def clients():
    from google.auth.transport.requests import AuthorizedSession
    from google.cloud import pubsub_v1, storage
    from validate_ingestion import credentials

    credential = credentials()
    return (AuthorizedSession(credential), storage.Client(project=PROJECT, credentials=credential),
            pubsub_v1.PublisherClient(credentials=credential), pubsub_v1.SubscriberClient(credentials=credential))


def api(http, method, url, *, allow_missing=False, **kwargs):
    response = http.request(method, url, timeout=30, **kwargs)
    if allow_missing and response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json() if response.content else {}


def operation(http, document):
    deadline = time.monotonic() + 600
    while not document.get("done"):
        if time.monotonic() >= deadline:
            raise RuntimeError("Probe operation timed out; retain receipt and reconcile before cleanup")
        time.sleep(5)
        document = api(http, "GET", "https://cloudfunctions.googleapis.com/v2/" + document["name"])
    if document.get("error"):
        raise RuntimeError("Probe operation failed; inspect its private receipt")


def grant(http, resource, member, role):
    before = api(http, "GET", resource + ":getIamPolicy")
    bindings = before.setdefault("bindings", [])
    match = next((b for b in bindings if b["role"] == role and not b.get("condition")), None)
    if match is None:
        match = {"role": role, "members": []}
        bindings.append(match)
    if member not in match["members"]:
        match["members"].append(member)
        api(http, "POST", resource + ":setIamPolicy", json={"policy": before})


def topology(http, function):
    meta = api(http, "GET", f"https://cloudfunctions.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/functions/{function}")
    trigger = api(http, "GET", "https://eventarc.googleapis.com/v1/" + meta["eventTrigger"]["trigger"])
    sub = api(http, "GET", "https://pubsub.googleapis.com/v1/" + trigger["transport"]["pubsub"]["subscription"])
    return {"function": meta, "trigger": trigger, "subscription": sub}


def configure(http, topology_record, destination):
    subscription = topology_record["subscription"]["name"]
    agent = f"serviceAccount:service-{NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"
    grant(http, f"https://pubsub.googleapis.com/v1/projects/{PROJECT}/topics/{destination}", agent, "roles/pubsub.publisher")
    grant(http, "https://pubsub.googleapis.com/v1/" + subscription, agent, "roles/pubsub.subscriber")
    api(http, "PATCH", "https://pubsub.googleapis.com/v1/" + subscription, json={
        "subscription": {"name": subscription,
                         "retryPolicy": {"minimumBackoff": "10s", "maximumBackoff": "60s"},
                         "deadLetterPolicy": {"deadLetterTopic": f"projects/{PROJECT}/topics/{destination}", "maxDeliveryAttempts": 5}},
        "updateMask": "retry_policy,dead_letter_policy",
    })


def prepare(path):
    if path.exists():
        raise ValueError("Refusing to replace a probe receipt")
    token = datetime.now(timezone.utc).strftime("%Y%m%d") + "_" + uuid.uuid4().hex[:8]
    state = {"token": token, "names": names(token), "phase": "preparing"}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2))
    n = state["names"]
    http, gcs, publisher, subscriber = clients()
    for name in (n["topic"], n["dead_letter"]):
        publisher.create_topic(request={"name": f"projects/{PROJECT}/topics/{name}",
                                        "labels": {"app": "cocoa-prices", "purpose": "delivery-validation"}}, timeout=30, retry=None)
    subscriber.create_subscription(request={"name": f"projects/{PROJECT}/subscriptions/{n['dead_letter_sub']}",
                                            "topic": f"projects/{PROJECT}/topics/{n['dead_letter']}",
                                            "ack_deadline_seconds": 60, "message_retention_duration": {"seconds": 86400},
                                            "expiration_policy": {}, "labels": {"app": "cocoa-prices", "purpose": "delivery-validation"}}, timeout=30, retry=None)
    contents = {"main.py": PROBE.encode(), "requirements.txt": b"functions-framework==3.8.3\ngoogle-cloud-storage==2.19.0\n",
                "request_policy.py": (ROOT / "functions/shared/request_policy.py").read_bytes()}
    stream = io.BytesIO()
    with ZipFile(stream, "w", compression=ZIP_STORED) as archive:
        for name, value in sorted(contents.items()):
            archive.writestr(name, value)
    data = stream.getvalue()
    digest = hashlib.sha256(data).hexdigest()
    bucket = gcs.bucket(f"gcf-v2-sources-{NUMBER}-{REGION}")
    blob = bucket.blob(n["function"] + "/probe-" + digest + ".zip")
    blob.upload_from_string(data, content_type="application/zip", if_generation_match=0, timeout=30)
    state["source"] = {"bucket": bucket.name, "object": blob.name, "generation": str(blob.generation), "sha256": digest}
    path.write_text(json.dumps(state, indent=2))
    document = api(http, "POST", f"https://cloudfunctions.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/functions",
                   params={"functionId": n["function"]}, json={
        "name": f"projects/{PROJECT}/locations/{REGION}/functions/{n['function']}",
        "labels": {"app": "cocoa-prices", "purpose": "delivery-validation"},
        "buildConfig": {"runtime": "python311", "entryPoint": "delivery_probe",
                        "source": {"storageSource": {"bucket": bucket.name, "object": blob.name, "generation": str(blob.generation)}},
                        "serviceAccount": f"projects/{PROJECT}/serviceAccounts/{ACCOUNT}"},
        "serviceConfig": {"maxInstanceCount": 1, "minInstanceCount": 0, "maxInstanceRequestConcurrency": 1,
                          "availableMemory": "256M", "availableCpu": "0.1666", "timeoutSeconds": 30,
                          "serviceAccountEmail": ACCOUNT,
                          "environmentVariables": {"PROBE_BUCKET": BUCKET, "PROBE_PREFIX": n["prefix"]}},
        "eventTrigger": {"triggerRegion": REGION, "eventType": "google.cloud.pubsub.topic.v1.messagePublished",
                         "pubsubTopic": f"projects/{PROJECT}/topics/{n['topic']}",
                         "serviceAccountEmail": ACCOUNT, "retryPolicy": "RETRY_POLICY_DO_NOT_RETRY"},
    })
    state["create_operation"] = document["name"]
    path.write_text(json.dumps(state, indent=2))
    operation(http, document)
    before = topology(http, n["function"])
    service = before["function"]["serviceConfig"]["service"]
    grant(http, "https://run.googleapis.com/v2/" + service, "serviceAccount:" + ACCOUNT, "roles/run.invoker")
    configure(http, before, n["dead_letter"])
    state["before_retry_toggle"] = before
    path.write_text(json.dumps(state, indent=2))
    event = dict(before["function"]["eventTrigger"])
    event.pop("trigger", None)
    event["retryPolicy"] = "RETRY_POLICY_RETRY"
    document = api(http, "PATCH", f"https://cloudfunctions.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/functions/{n['function']}",
                   params={"updateMask": "event_trigger.retry_policy"}, json={"eventTrigger": event})
    state["toggle_operation"] = document["name"]
    path.write_text(json.dumps(state, indent=2))
    operation(http, document)
    after = topology(http, n["function"])
    configure(http, after, n["dead_letter"])
    assert after["function"]["eventTrigger"]["retryPolicy"] == "RETRY_POLICY_RETRY"
    state.update(after_retry_toggle=after, retry_toggle_kept_transport=before["subscription"]["name"] == after["subscription"]["name"], phase="ready")
    path.write_text(json.dumps(state, indent=2))
    print(json.dumps({"phase": "ready", "token": token, "retry_toggle_kept_transport": state["retry_toggle_kept_transport"]}), flush=True)


def counters(gcs, prefix):
    return [json.loads(blob.download_as_bytes(timeout=10, retry=None)) for blob in gcs.list_blobs(BUCKET, prefix=prefix + "attempts/", timeout=10)]


def wait_counter(gcs, prefix, scenario, deadline, *, replay=False):
    latest = None
    while time.monotonic() < deadline:
        matches = [r for r in counters(gcs, prefix) if r["scenario"] == scenario]
        if matches:
            record = matches[0]
            if record["attempts"] != latest:
                print(json.dumps({"scenario": scenario, "attempts": record["attempts"], "accepted": record["accepted"]}), flush=True)
                latest = record["attempts"]
            if record["accepted"] and (not replay or len(record["event_ids"]) >= 2):
                return record
        time.sleep(5)
    raise RuntimeError("Probe did not complete within its bounded validation window")


def original_payload_matches(data, expected):
    if data == expected:
        return True
    try:
        value = json.loads(data)
        for _ in range(3):
            if isinstance(value, dict) and "data" in value:
                return base64.b64decode(value["data"], validate=True) == expected
            value = value.get("message", {}) if isinstance(value, dict) else {}
    except (ValueError, TypeError):
        pass
    return False


def exercise(path):
    state = checked(path)
    if state["phase"] != "ready":
        raise ValueError("Probe must be ready; do not replay a partially completed exercise automatically")
    http, gcs, publisher, subscriber = clients()
    n = state["names"]
    live = topology(http, n["function"])
    assert live["function"]["state"] == "ACTIVE" and live["function"]["buildConfig"]["runtime"] == "python311"
    assert live["function"]["eventTrigger"]["retryPolicy"] == "RETRY_POLICY_RETRY"
    assert live["subscription"]["deadLetterPolicy"]["maxDeliveryAttempts"] == 5
    assert live["subscription"]["retryPolicy"] == {"minimumBackoff": "10s", "maximumBackoff": "60s"}
    src = live["function"]["buildConfig"].get("sourceProvenance", {}).get("resolvedStorageSource") or live["function"]["buildConfig"]["source"]["storageSource"]
    payload = gcs.bucket(src["bucket"]).blob(src["object"], generation=int(src["generation"])).download_as_bytes(timeout=30)
    assert hashlib.sha256(payload).hexdigest() == state["source"]["sha256"]
    state["live_topology_before_exercise"] = live
    state["phase"] = "exercising"
    path.write_text(json.dumps(state, indent=2))
    def publish(scenario):
        payload = json.dumps({"scenario": scenario}).encode()
        return publisher.publish(f"projects/{PROJECT}/topics/{n['topic']}", payload,
                                 acquisition_key=scenario + "-" + state["token"]).result(timeout=10)
    state["transient_message_id"] = publish("transient")
    path.write_text(json.dumps(state, indent=2))
    transient = wait_counter(gcs, n["prefix"], "transient", time.monotonic() + 300)
    assert transient["attempts"] >= 2 and len(transient["event_ids"]) == 1
    state["transient"] = transient
    state["permanent_message_id"] = publish("permanent")
    path.write_text(json.dumps(state, indent=2))
    deadline = time.monotonic() + 600
    destination = f"projects/{PROJECT}/subscriptions/{n['dead_letter_sub']}"
    delivery = None
    while time.monotonic() < deadline:
        result = subscriber.pull(request={"subscription": destination, "max_messages": 1, "return_immediately": True}, timeout=10, retry=None)
        if result.received_messages:
            delivery = result.received_messages[0]
            break
        time.sleep(5)
    if delivery is None:
        raise RuntimeError("Poison message did not reach the isolated dead-letter destination")
    expected = json.dumps({"scenario": "permanent"}).encode()
    if not original_payload_matches(delivery.message.data, expected):
        raise RuntimeError("Unexpected dead-letter envelope; retain it for inspection")
    subscriber.modify_ack_deadline(request={"subscription": destination, "ack_ids": [delivery.ack_id], "ack_deadline_seconds": 120}, timeout=10, retry=None)
    permanent = next(r for r in counters(gcs, n["prefix"]) if r["scenario"] == "permanent")
    assert not permanent["accepted"] and permanent["attempts"] >= 2
    state["permanent"] = permanent
    state["dead_letter"] = {"message_id": delivery.message.message_id, "attributes": dict(delivery.message.attributes), "original_payload_verified": True}
    path.write_text(json.dumps(state, indent=2))
    gcs.bucket(BUCKET).blob(n["prefix"] + "accept-permanent").upload_from_string(b"accepted controlled remediation", if_generation_match=0, timeout=10)
    state["replay_message_id"] = publish("permanent")
    path.write_text(json.dumps(state, indent=2))
    state["replay"] = wait_counter(gcs, n["prefix"], "permanent", time.monotonic() + 300, replay=True)
    assert state["replay"]["acquisition_id"] == permanent["acquisition_id"]
    subscriber.acknowledge(request={"subscription": destination, "ack_ids": [delivery.ack_id]}, timeout=10, retry=None)
    state["phase"] = "verified"
    path.write_text(json.dumps(state, indent=2))
    print(json.dumps({"phase": "verified", "transient_attempts": transient["attempts"],
                      "dead_letter_attempts": permanent["attempts"], "replay_kept_acquisition_id": True}), flush=True)


def cleanup(path):
    state = checked(path)
    http, gcs, publisher, subscriber = clients()
    n = state["names"]
    function_url = f"https://cloudfunctions.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/functions/{n['function']}"
    meta = api(http, "GET", function_url, allow_missing=True)
    if meta is not None:
        if meta.get("labels", {}).get("purpose") != "delivery-validation":
            raise ValueError("Refusing to delete a function without the validation label")
        operation(http, api(http, "DELETE", function_url))
    if api(http, "GET", function_url, allow_missing=True) is not None:
        raise RuntimeError("Probe function deletion has not completed")
    transport = state.get("after_retry_toggle", {}).get("subscription", {}).get("name")
    if transport:
        if not transport.startswith(f"projects/{PROJECT}/subscriptions/eventarc-{REGION}-{n['function']}-"):
            raise ValueError("Unexpected probe transport name")
        if api(http, "GET", "https://pubsub.googleapis.com/v1/" + transport, allow_missing=True) is not None:
            raise RuntimeError("Eventarc transport cleanup has not completed; retry cleanup later")
    for kind, name in (("subscriptions", n["dead_letter_sub"]), ("topics", n["topic"]), ("topics", n["dead_letter"])):
        url = f"https://pubsub.googleapis.com/v1/projects/{PROJECT}/{kind}/{name}"
        meta = api(http, "GET", url, allow_missing=True)
        if meta is not None:
            if meta.get("labels", {}).get("purpose") != "delivery-validation":
                raise ValueError("Refusing to delete an unlabelled Pub/Sub resource")
            api(http, "DELETE", url)
    for blob in gcs.list_blobs(BUCKET, prefix=n["prefix"], versions=True, timeout=10):
        if not blob.name.startswith(n["prefix"]):
            raise ValueError("Unexpected validation object prefix")
        blob.delete(if_generation_match=blob.generation, timeout=10)
    source = state.get("source")
    if source:
        if not source["object"].startswith(n["function"] + "/probe-"):
            raise ValueError("Unexpected probe source path")
        api_blob = gcs.bucket(source["bucket"]).blob(source["object"], generation=int(source["generation"]))
        if api_blob.exists(timeout=10):
            api_blob.delete(if_generation_match=int(source["generation"]), timeout=10)
    state["resources_deleted"] = True
    path.write_text(json.dumps(state, indent=2))
    print(json.dumps({"resources_deleted": True, "token": state["token"]}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("prepare", "exercise", "cleanup"))
    parser.add_argument("--receipt", required=True, type=Path)
    args = parser.parse_args()
    {"prepare": prepare, "exercise": exercise, "cleanup": cleanup}[args.phase](args.receipt)


if __name__ == "__main__":
    main()
