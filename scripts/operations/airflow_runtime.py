"""Bounded VM startup/readiness and a recorded, recoverable hard shutdown."""

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from urllib.request import Request, urlopen

PROJECT = "cocoa-prices-430315"
COMPOSE = ["docker", "compose"]
CONTAINER = "airflow-airflow-scheduler-1"
READINESS = """
from airflow.models import Variable
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from google.auth.transport.requests import Request
credentials = GoogleBaseHook(gcp_conn_id='gcp_default').get_credentials()
credentials.refresh(Request())
assert credentials.valid
assert Variable.get('COCOA_FN_URL').startswith('https://')
assert Variable.get('BQ_PROC') == 'analytics_delivery.run_daily_pipeline'
print('READY')
"""
ACTIVE_RUNS = """
import json
from airflow.models import DagRun
from airflow.utils.session import create_session
with create_session() as session:
    runs = session.query(DagRun).filter(DagRun.dag_id=='cocoa_daily',
        DagRun.state.in_(['running','queued'])).limit(20).all()
    print(json.dumps([{'run_id':r.run_id,'state':r.state} for r in runs]))
"""


def command(args, timeout, env=None):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, env=env)
    if result.returncode:
        # CLI errors may contain environment/credential details. Keep them private.
        raise RuntimeError("Runtime command failed: " + args[0])
    return result.stdout.strip()


def emit(code, severity, **details):
    payload = {"event": "operational_alert" if severity == "ERROR" else "runtime_ready",
               "code": code, **details}
    print(json.dumps({"severity": severity, **payload}), flush=True)
    try:
        request = Request("http://metadata.google.internal/computeMetadata/v1/instance/"
                          "service-accounts/default/token", headers={"Metadata-Flavor": "Google"})
        with urlopen(request, timeout=5) as response:
            token = json.load(response)["access_token"]
        body = {"logName": f"projects/{PROJECT}/logs/cocoa-operations",
                "resource": {"type": "global", "labels": {"project_id": PROJECT}},
                "entries": [{"severity": severity, "jsonPayload": payload,
                             "timestamp": datetime.now(timezone.utc).isoformat()}]}
        request = Request("https://logging.googleapis.com/v2/entries:write",
                          data=json.dumps(body).encode(), headers={
                              "Authorization": "Bearer " + token, "Content-Type": "application/json"})
        with urlopen(request, timeout=5):
            pass
    except Exception:
        print("Central runtime log unavailable; use the systemd journal", flush=True)


def start():
    key = command(["gcloud", "secrets", "versions", "access", "latest",
                   "--secret=airflow-fernet-key", "--project=" + PROJECT], 30)
    if not key:
        raise RuntimeError("Fernet key unavailable")
    env = dict(os.environ, AIRFLOW__CORE__FERNET_KEY=key)
    command(COMPOSE + ["up", "-d", "--no-build", "--pull", "never", "--wait", "--wait-timeout", "240"], 250, env)
    result = command(COMPOSE + ["exec", "-T", "airflow-scheduler", "python", "-c", READINESS], 30, env)
    if result.splitlines()[-1:] != ["READY"]:
        raise RuntimeError("Airflow credential/configuration readiness failed")
    emit("startup_ready", "INFO", message="Compose health, credential refresh and required configuration passed")


def stop():
    try:
        runs = json.loads(command(["docker", "exec", CONTAINER, "python", "-c", ACTIVE_RUNS], 15))
        if runs:
            emit("shutdown_interrupted", "ERROR", runs=runs,
                 message="Hard VM cost cutoff: reconcile deterministic warehouse jobs and retained run state before recovery")
    except Exception:
        emit("shutdown_state_unknown", "ERROR",
             message="Could not inspect Airflow at shutdown; reconcile retained metadata and warehouse jobs")
    # Compose parses required interpolation even for stop. This placeholder is
    # never installed into a container; stopping must not depend on Secret Manager.
    env = dict(os.environ, AIRFLOW__CORE__FERNET_KEY="unused-for-stop")
    command(COMPOSE + ["stop", "--timeout", "20"], 45, env)


def main():
    action = sys.argv[1] if len(sys.argv) == 2 else ""
    if action not in {"start", "stop"} or sys.version_info[:2] != (3, 11):
        raise SystemExit("Use Python 3.11 and action start or stop")
    try:
        {"start": start, "stop": stop}[action]()
    except Exception:
        emit(action + "_failed", "ERROR", message="Airflow runtime action failed; inspect systemd state and recover")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
