"""Encrypted VM-state backups; SDK/crypto work uses the installed Airflow image."""

import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import runpy
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import uuid
from datetime import datetime, timezone

PROJECT = "cocoa-prices-430315"
PROJECT_NUMBER = "494039866722"
BUCKET = PROJECT + "-runtime-backups"
AIRFLOW = "airflow-airflow-scheduler-1"
POSTGRES = "airflow-postgres-1"
MAX_STATE = 64 * 1024**2
MAX_IMAGE = 2 * 1024**3
MAX_IMAGES = 4


def sha(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    with open(path, "rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def command(args, timeout=60, data=None, env=None):
    result = subprocess.run(args, input=data, capture_output=True, timeout=timeout, env=env)
    if result.returncode:
        raise RuntimeError("Recovery command failed: " + Path(args[0]).name)
    return result.stdout


def worker_call(mode, data):
    version = os.environ.get("COCOA_FERNET_VERSION", "latest")
    if version != "latest" and not version.isdigit():
        raise RuntimeError("Invalid Fernet version reference")
    return command(["docker", "exec", "-i", "--env", "COCOA_FERNET_VERSION=" + version, AIRFLOW, "python", "-c",
                    Path(__file__).read_text(), "worker", mode], 180, data)


def worker_json(mode, data):
    output = worker_call(mode, json.dumps(data).encode())
    return json.loads(next(line[7:] for line in output.splitlines() if line.startswith(b"RESULT=")))


def worker(mode):
    from cryptography.fernet import Fernet
    from google.cloud import secretmanager, storage

    bucket = storage.Client(project=PROJECT).bucket(BUCKET)
    secret_client = secretmanager.SecretManagerServiceClient()

    def access(name):
        response = secret_client.access_secret_version(request={"name": name}, timeout=30)
        return response.name, response.payload.data.strip()

    def describe(blob):
        blob.reload(timeout=30)
        return {"object": blob.name, "generation": str(blob.generation),
                "sha256": (blob.metadata or {}).get("sha256"), "size": blob.size}

    if mode == "images":
        from airflow.models import DagRun
        from airflow.utils.session import create_session

        ids = json.load(sys.stdin)
        existing = list(bucket.list_blobs(prefix="images/", max_results=MAX_IMAGES + 1, timeout=30))
        missing = [value for value in ids if not any(b.name == "images/" + value[7:] + ".tar.gz" for b in existing)]
        if len(existing) + len(missing) > MAX_IMAGES:
            raise RuntimeError("Image retention ceiling reached; review references before cleanup")
        images = {value: describe(next(b for b in existing if b.name == "images/" + value[7:] + ".tar.gz"))
                  for value in ids if value not in missing}
        with create_session() as session:
            run = session.query(DagRun).filter(DagRun.dag_id == "cocoa_daily", DagRun.state == "success").order_by(DagRun.end_date.desc()).first()
            retained = None if run is None else {"run_id": run.run_id, "state": run.state,
                                                 "tasks": {t.task_id: t.state for t in run.get_task_instances()}}
        print("RESULT=" + json.dumps({"images": images, "retained_run": retained}))
    elif mode == "seal":
        import hmac

        data = sys.stdin.buffer.read(MAX_STATE + 1)
        if len(data) > MAX_STATE:
            raise RuntimeError("State archive exceeds 64 MiB ceiling")
        version, key = access(f"projects/{PROJECT}/secrets/airflow-fernet-key/versions/" + os.environ["COCOA_FERNET_VERSION"])
        if not hmac.compare_digest(key, os.environ["AIRFLOW__CORE__FERNET_KEY"].strip().encode()):
            raise RuntimeError("Running Fernet key differs from Secret Manager; reconcile rotation first")
        sealed = Fernet(key.split(b",")[0]).encrypt(data)
        name = "runtime/" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ_") + uuid.uuid4().hex
        blob = bucket.blob(name + ".tar.gz.fernet")
        blob.metadata = {"sha256": sha(sealed), "secret_version": version}
        blob.upload_from_string(sealed, content_type="application/octet-stream", if_generation_match=0, timeout=60)
        receipt = {"version": 1, "created_at": datetime.now(timezone.utc).isoformat(),
                   "bucket": BUCKET, "state": describe(blob), "secret_version": version}
        receipt_blob = bucket.blob(name + ".json")
        receipt_blob.upload_from_string(json.dumps(receipt), content_type="application/json", if_generation_match=0, timeout=30)
        print("RESULT=" + json.dumps({**receipt, "receipt_object": receipt_blob.name, "receipt_generation": str(receipt_blob.generation)}))
    else:
        raise RuntimeError("Unknown worker mode")


def export_image(image, destination):
    if shutil.disk_usage(destination.parent).free < MAX_IMAGE + MAX_STATE * 3:
        raise RuntimeError("Insufficient temporary disk headroom")
    process = subprocess.Popen(["docker", "save", image], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    try:
        with gzip.open(destination, "wb", compresslevel=1) as stream:
            while chunk := process.stdout.read(1024**2):
                stream.write(chunk)
                if destination.stat().st_size > MAX_IMAGE:
                    raise RuntimeError("Image archive exceeds two GiB ceiling")
        if process.wait(timeout=15):
            raise RuntimeError("Image export failed")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=15)


def runtime_files():
    base = Path("/opt/airflow")
    files = [p for p in base.iterdir() if p.name in {".env", "Dockerfile", "requirements.txt"}
             or p.name in {"docker-compose.yaml", "docker-compose.yml", "docker-compose.override.yaml",
                           "docker-compose.override.yml", "compose.yaml", "compose.yml", "compose.override.yaml", "compose.override.yml"}]
    for folder in ("config", "dags", "plugins"):
        files.extend(p for p in (base / folder).rglob("*") if p.is_file() and "__pycache__" not in p.parts)
    files.extend(Path("/etc/systemd/system") / name for name in
                 ("airflow-compose.service", "cocoa-airflow-backup.service", "cocoa-airflow-backup.timer"))
    files.extend(Path("/usr/local/lib/cocoa") / name for name in ("airflow_runtime.py", "airflow_backup.py", "airflow_restore.py"))
    for unit in ("airflow-compose.service", "cocoa-airflow-backup.service"):
        pin = Path("/etc/systemd/system") / (unit + ".d") / "recovery-key.conf"
        if pin.exists():
            files.append(pin)
    for path in files:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 8 * 1024**2:
            raise RuntimeError("Runtime file needs explicit recovery mapping or exceeds size ceiling")
    return sorted(files)


def backup():
    started = datetime.now(timezone.utc).isoformat()
    ids = {name: command(["docker", "inspect", "--format={{.Image}}", container]).decode().strip()
           for name, container in (("airflow", AIRFLOW), ("postgres", POSTGRES))}
    if any(not value.startswith("sha256:") or len(value) != 71 for value in ids.values()):
        raise RuntimeError("Unexpected runtime image identity")
    context = worker_json("images", list(ids.values()))
    with tempfile.TemporaryDirectory(prefix="cocoa-backup-") as directory:
        temporary = Path(directory)
        images = context["images"]
        for image in set(ids.values()) - images.keys():
            print(json.dumps({"event": "backup_stage", "stage": "image_export", "image_id": image}), flush=True)
            archive = temporary / (image[7:] + ".tar.gz")
            export_image(image, archive)
            digest = file_sha(archive)
            name = "images/" + archive.name
            print(json.dumps({"event": "backup_stage", "stage": "image_upload", "image_id": image}), flush=True)
            command(["gcloud", "storage", "cp", "--if-generation-match=0", "--custom-metadata=sha256=" + digest,
                     str(archive), f"gs://{BUCKET}/{name}"], 300,
                    env=dict(os.environ, CLOUDSDK_STORAGE_PARALLEL_COMPOSITE_UPLOAD_ENABLED="false"))
            # Read the authoritative generation through the existing SDK worker.
            images = worker_json("images", list(ids.values()))["images"]
            archive.unlink()
        dump = temporary / "postgres.dump"
        print(json.dumps({"event": "backup_stage", "stage": "postgres_dump"}), flush=True)
        dump_at = datetime.now(timezone.utc).isoformat()
        with open(dump, "wb") as stream:
            result = subprocess.run(["docker", "exec", POSTGRES, "pg_dump", "-U", "airflow", "-d", "airflow",
                                     "--format=custom", "--no-owner", "--no-acl"], stdout=stream, stderr=subprocess.PIPE, timeout=120)
        if result.returncode or dump.stat().st_size > 128 * 1024**2:
            raise RuntimeError("Metadata dump failed or exceeds ceiling")
        manifest = {"version": 1, "started_at": started, "database_dump_at": dump_at,
                    "images": {name: {"id": image, **images[image]} for name, image in ids.items()},
                    "retained_run": context["retained_run"], "files": {"postgres.dump": file_sha(dump)}}
        archive = temporary / "state.tar.gz"
        with tarfile.open(archive, "w:gz") as tar:
            tar.add(dump, arcname="postgres.dump")
            for path in runtime_files():
                name = "runtime/" + path.as_posix().lstrip("/")
                manifest["files"][name] = file_sha(path)
                tar.add(path, arcname=name, recursive=False)
            body = json.dumps(manifest, sort_keys=True).encode()
            member = tarfile.TarInfo("manifest.json")
            member.size = len(body)
            tar.addfile(member, io.BytesIO(body))
        if archive.stat().st_size > MAX_STATE:
            raise RuntimeError("State archive exceeds 64 MiB ceiling")
        print(json.dumps({"event": "backup_stage", "stage": "state_seal"}), flush=True)
        receipt = json.loads(next(line[7:] for line in worker_call("seal", archive.read_bytes()).splitlines()
                                  if line.startswith(b"RESULT=")))
    print(json.dumps({"event": "backup_completed", **receipt, "dump_at": manifest["database_dump_at"]}), flush=True)
    return receipt


def extract_verified(data, destination):
    """Reject paths/links and verify all bytes before a restore may use them."""
    destination = Path(destination)
    if destination.is_symlink() or (destination.exists() and any(destination.iterdir())):
        raise RuntimeError("Recovery destination must be empty and private")
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        members = tar.getmembers()
        names = [member.name for member in members]
        if len(members) > 10000 or len(set(names)) != len(names) or sum(m.size for m in members) > 256 * 1024**2:
            raise RuntimeError("Archive membership exceeds recovery limits")
        for member in members:
            path = PurePosixPath(member.name)
            if (not member.isfile() or path.is_absolute() or ".." in path.parts
                    or "\\" in member.name or ":" in member.name or str(path) == "."):
                raise RuntimeError("Unsafe recovery archive member")
        manifest = json.load(tar.extractfile("manifest.json"))
        if manifest.get("version") != 1 or set(names) != set(manifest["files"]) | {"manifest.json"}:
            raise RuntimeError("Recovery manifest membership mismatch")
        for member in members:
            content = tar.extractfile(member).read()
            if member.name != "manifest.json" and sha(content) != manifest["files"][member.name]:
                raise RuntimeError("Recovery file hash mismatch")
        for member in members:
            target = destination / member.name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(tar.extractfile(member).read())
    return manifest


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "worker":
        worker(sys.argv[2])
        return
    if sys.version_info[:2] != (3, 11) or sys.argv[1:] != ["backup"]:
        raise SystemExit("Use Python 3.11: airflow_backup.py backup")
    def interrupted(*_):
        raise RuntimeError("Backup interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    try:
        backup()
    except Exception:
        runpy.run_path("/usr/local/lib/cocoa/airflow_runtime.py")["emit"](
            "backup_failed", "ERROR", message="Airflow backup failed or was interrupted; inspect the backup service")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
