"""Restore downloaded backup state into an isolated, paused Docker runtime only."""

import argparse
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import tempfile
import time
import uuid

from airflow_backup import BUCKET, MAX_IMAGE, MAX_STATE, PROJECT, PROJECT_NUMBER, command, extract_verified, file_sha

# Public, immutable bootstrap with Airflow's required Fernet dependency. Restore
# can start on a replacement VM with Docker/gcloud/Python, without live Airflow.
RECOVERY_IMAGE = "apache/airflow:2.10.2-python3.11@sha256:7c68d8ac3cf1b2c69eeb91da64c1578dc8376d33abf34267bf6e547d071cae39"


def read_backup(reference, temporary):
    if (not reference["object"].startswith("runtime/") or not reference["object"].endswith(".json")
            or int(reference["generation"]) <= 0):
        raise RuntimeError("Unexpected receipt reference")
    receipt = json.loads(command(["gcloud", "storage", "cat", f"gs://{BUCKET}/{reference['object']}#{reference['generation']}"]))
    prefixes = [f"projects/{project}/secrets/airflow-fernet-key/versions/" for project in (PROJECT, PROJECT_NUMBER)]
    prefix = next((value for value in prefixes if receipt["secret_version"].startswith(value)), "")
    if (receipt.get("version") != 1 or receipt["bucket"] != BUCKET
            or not prefix
            or not receipt["secret_version"][len(prefix):].isdigit()):
        raise RuntimeError("Unexpected backup or immutable key reference")
    state = receipt["state"]
    if not state["object"].startswith("runtime/") or not 0 < state["size"] <= MAX_STATE * 2:
        raise RuntimeError("Unexpected state object or size")
    encrypted = Path(temporary) / "state.fernet"
    command(["gcloud", "storage", "cp", f"gs://{BUCKET}/{state['object']}#{state['generation']}", str(encrypted)], 180)
    if encrypted.stat().st_size != state["size"] or file_sha(encrypted) != state["sha256"]:
        raise RuntimeError("Ciphertext hash/size mismatch")
    key = command(["gcloud", "secrets", "versions", "access", receipt["secret_version"][len(prefix):],
                   "--secret=airflow-fernet-key", "--project=" + PROJECT]).strip().decode()
    env = dict(os.environ, COCOA_BACKUP_KEY=key)
    decrypt = "import os,sys; from cryptography.fernet import Fernet; sys.stdout.buffer.write(Fernet(os.environ['COCOA_BACKUP_KEY'].encode().split(b',')[0]).decrypt(sys.stdin.buffer.read()))"
    data = command(["docker", "run", "--rm", "-i", "--env", "COCOA_BACKUP_KEY", "--entrypoint=python",
                    RECOVERY_IMAGE, "-c", decrypt], 300, encrypted.read_bytes(), env)
    if len(data) > MAX_STATE:
        raise RuntimeError("Decrypted archive exceeds size ceiling")
    return receipt, data, key


def restore_check(receipt_reference):
    started = time.monotonic()
    identity = "cocoa-restore-" + uuid.uuid4().hex[:12]
    postgres, scheduler = identity + "-pg", identity + "-scheduler"
    created = {"network": False, "volume": False, "postgres": False, "scheduler": False}
    with tempfile.TemporaryDirectory(prefix=identity + "-") as directory:
        temporary = Path(directory)
        receipt, data, key = read_backup(receipt_reference, temporary)
        root = temporary / "verified"
        manifest = extract_verified(data, root)
        del data
        # Loading archives by immutable image ID does not retag production images.
        for image in manifest["images"].values():
            if (image["object"] != "images/" + image["id"][7:] + ".tar.gz"
                    or not 0 < image["size"] <= MAX_IMAGE or not image["sha256"]):
                raise RuntimeError("Unexpected recovery image reference")
            archive = root / "image.tar.gz"
            command(["gcloud", "storage", "cp", f"gs://{BUCKET}/{image['object']}#{image['generation']}", str(archive)], 300)
            if archive.stat().st_size != image["size"] or file_sha(archive) != image["sha256"]:
                raise RuntimeError("Recovery image hash/size mismatch")
            command(["docker", "load", "--input", str(archive)], 300)
            command(["docker", "image", "inspect", image["id"]])
            archive.unlink()
        password = secrets.token_urlsafe(32)
        env = dict(os.environ, AIRFLOW__CORE__FERNET_KEY=key,
                   AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"postgresql+psycopg2://airflow:{password}@{postgres}/airflow",
                   AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK="true", AIRFLOW__CORE__LOAD_EXAMPLES="false")
        try:
            command(["docker", "network", "create", "--label", "app=cocoa-restore-check", identity])
            created["network"] = True
            command(["docker", "volume", "create", "--label", "app=cocoa-restore-check", identity])
            created["volume"] = True
            pg_env = dict(os.environ, POSTGRES_PASSWORD=password)
            command(["docker", "run", "-d", "--name", postgres, "--label", "app=cocoa-restore-check",
                     "--memory=384m",
                     "--network", identity, "-e", "POSTGRES_PASSWORD", "-e", "POSTGRES_USER=airflow",
                     "-e", "POSTGRES_DB=airflow", "-v", identity + ":/var/lib/postgresql/data",
                     manifest["images"]["postgres"]["id"]], env=pg_env)
            created["postgres"] = True
            for attempt in range(30):
                result = subprocess.run(["docker", "exec", postgres, "pg_isready", "-U", "airflow"], capture_output=True, timeout=10)
                if result.returncode == 0:
                    break
                time.sleep(2)
            else:
                raise RuntimeError("Isolated PostgreSQL did not become ready")
            command(["docker", "cp", str(root / "postgres.dump"), postgres + ":/tmp/restore.dump"])
            command(["docker", "exec", postgres, "pg_restore", "-U", "airflow", "-d", "airflow",
                     "--no-owner", "--no-acl", "/tmp/restore.dump"], 120)
            flags = ["--network", identity, "--user=0:0", "--env", "AIRFLOW__CORE__FERNET_KEY",
                     "--env", "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "--env", "AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK",
                     "--env", "AIRFLOW__CORE__LOAD_EXAMPLES", "-v", str(root / "runtime/opt/airflow/dags") + ":/restore-dags:ro"]
            for folder, target in (("config", "/restore-config"), ("plugins", "/restore-plugins")):
                source = root / "runtime/opt/airflow" / folder
                if source.is_dir():
                    flags.extend(["-v", str(source) + ":" + target + ":ro"])
            if (root / "runtime/opt/airflow/config/airflow.cfg").is_file():
                flags.extend(["--env", "AIRFLOW_CONFIG=/restore-config/airflow.cfg"])
            flags.extend(["--env", "AIRFLOW__CORE__PLUGINS_FOLDER=/restore-plugins"])
            check = """
import json,runpy
from airflow.models import Connection,DagModel,DagRun,TaskInstance,Variable
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.session import create_session
from google.auth.transport.requests import Request
expected=json.loads(__EXPECTED__)
with create_session() as session:
    assert session.query(Variable).count()>0 and session.query(Connection).count()>0
    for value in session.query(Variable).all():value.get_val()
    for value in session.query(Connection).all():value.get_password();value.get_extra()
    if expected:
        run=session.query(DagRun).filter(DagRun.dag_id=='cocoa_daily',DagRun.run_id==expected['run_id']).one()
        assert run.state==expected['state']
        assert {t.task_id:t.state for t in run.get_task_instances()}==expected['tasks']
    # Safe mode applies to this isolated database only, before any scheduler runs.
    session.query(DagModel).update({'is_paused':True})
    session.query(DagRun).filter(DagRun.state.in_(['running','queued'])).update({'state':'failed'})
    session.query(TaskInstance).filter(TaskInstance.state.in_(['running','queued','scheduled','up_for_retry','up_for_reschedule'])).update({'state':'failed'})
    session.commit()
credentials=GoogleBaseHook(gcp_conn_id='gcp_default').get_credentials()
credentials.refresh(Request())
assert credentials.valid
dag=runpy.run_path('/restore-dags/cocoa_daily_dag.py')['dag']
assert dag.dag_id=='cocoa_daily' and len(dag.tasks)==9
print('RESTORED='+json.dumps({'metadata':'restored','fernet_decryption':'passed','credential_refresh':'passed','dag_import':'passed','paused':True}))
""".replace("__EXPECTED__", repr(json.dumps(manifest["retained_run"])))
            output = command(["docker", "run", "--rm", *flags,
                              manifest["images"]["airflow"]["id"], "python", "-c", check], 90, env=env)
            record = json.loads(next(line[9:] for line in output.splitlines() if line.startswith(b"RESTORED=")))
            command(["docker", "run", "-d", "--name", scheduler, "--label", "app=cocoa-restore-check",
                     "--memory=1536m",
                     *flags, "--env", "AIRFLOW__CORE__DAGS_FOLDER=/restore-dags",
                     manifest["images"]["airflow"]["id"], "scheduler"], env=env)
            created["scheduler"] = True
            health = "import urllib.request; assert urllib.request.urlopen('http://localhost:8974/health',timeout=3).status==200"
            for attempt in range(40):
                result = subprocess.run(["docker", "exec", scheduler, "python", "-c", health], capture_output=True, timeout=10)
                if result.returncode == 0:
                    break
                time.sleep(3)
            else:
                raise RuntimeError("Restored isolated scheduler did not become healthy")
            record.update(scheduler_health="passed", isolated_identity=identity,
                          receipt_reference=receipt_reference, backup_created_at=receipt["created_at"],
                          elapsed_seconds=round(time.monotonic() - started, 2))
        finally:
            failures = []
            for name, resource in (("scheduler", scheduler), ("postgres", postgres)):
                if created[name]:
                    try:
                        command(["docker", "rm", "-f", resource])
                    except Exception:
                        failures.append(resource)
            if created["volume"]:
                try:
                    command(["docker", "volume", "rm", identity])
                except Exception:
                    failures.append("volume:" + identity)
            if created["network"]:
                try:
                    command(["docker", "network", "rm", identity])
                except Exception:
                    failures.append("network:" + identity)
            if failures:
                raise RuntimeError("Isolated restore cleanup incomplete: " + ", ".join(failures))
    record["cleanup"] = "completed"
    print(json.dumps({"event": "restore_check_completed", **record}), flush=True)
    return record


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt-object", required=True)
    parser.add_argument("--receipt-generation", required=True, type=int)
    parser.add_argument("--extract-to", type=Path, help="Materialize verified files into a new private directory; no live restore")
    args = parser.parse_args()
    if sys.version_info[:2] != (3, 11):
        raise SystemExit("Use Python 3.11")
    os.umask(0o077)

    def interrupted(*_):
        raise RuntimeError("Restore check interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    reference = {"object": args.receipt_object, "generation": args.receipt_generation}
    if args.extract_to:
        if args.extract_to.exists():
            raise SystemExit("Recovery destination must not already exist")
        with tempfile.TemporaryDirectory(prefix="cocoa-materialize-") as temporary:
            receipt, data, _ = read_backup(reference, temporary)
            extract_verified(data, args.extract_to)
            (args.extract_to / "recovery-reference.json").write_text(json.dumps(receipt, indent=2))
            version = receipt["secret_version"].rsplit("/", 1)[1]
            (args.extract_to / "recovery-key.conf").write_text("[Service]\nEnvironment=COCOA_FERNET_VERSION=" + version + "\n")
        print("Verified recovery files materialized; no live runtime was changed.")
    else:
        restore_check(reference)
