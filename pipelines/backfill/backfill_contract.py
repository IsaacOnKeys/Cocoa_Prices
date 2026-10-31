"""Explicit immutable inputs and isolated, non-destructive Beam backfill outputs."""

import argparse
import csv
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime
import gzip
import hashlib
import io
import json
from pathlib import Path
import re

PROJECT = "cocoa-prices-430315"
TABLES = {"cocoa": ("cocoa", "invalid_cocoa"), "oil": ("brent_prices", "invalid_brent_prices"),
          "weather": ("precipitation", "invalid_precipitation")}
MAX_INPUT = 16 * 1024**2
SCRIPTS = {"cocoa": "clean_cocoa_prices.py", "oil": "clean_oil_eia.py", "weather": "clean_precipitation.py"}


@dataclass(frozen=True)
class BackfillRequest:
    feed: str
    input_uri: str
    generation: int
    sha256: str
    start: str
    end: str
    dataset: str
    manifest_output: str
    acquired_at: str | None = None
    transform_hashes: dict[str, str] | None = None

    def __post_init__(self):
        lower, upper = date.fromisoformat(self.start), date.fromisoformat(self.end)
        if (self.feed not in TABLES or lower.isoformat() != self.start or upper.isoformat() != self.end
                or not date(2014, 1, 1) <= lower <= upper <= date.today() or (upper - lower).days > 5000
                or not re.fullmatch(r"gs://raw-historic-data/[A-Za-z0-9_/.-]+", self.input_uri)
                or any(part in {"", ".", ".."} for part in self.input_uri.removeprefix("gs://raw-historic-data/").split("/"))
                or self.generation <= 0 or not re.fullmatch(r"[a-f0-9]{64}", self.sha256)
                or not re.fullmatch(PROJECT + r"\.backfill_[0-9]{8}_[a-f0-9]{8}", self.dataset)):
            raise ValueError("Expected bounded dates, an exact raw archive generation/hash and an isolated backfill dataset")
        if self.acquired_at is not None:
            acquired = datetime.fromisoformat(self.acquired_at)
            if acquired.tzinfo is None or upper > acquired.date():
                raise ValueError("Invalid source acquisition time")

    def table(self, invalid=False):
        return self.dataset.replace(".", ":", 1) + "." + TABLES[self.feed][int(invalid)]

    @property
    def token(self):
        return self.dataset.rsplit("_", 1)[1]

    @property
    def quarantine_prefix(self):
        return f"quarantine/v1/historical-backfill/{self.dataset.split('.')[1]}/{self.feed}/"


def verify_source(request):
    from google.cloud import storage

    name = request.input_uri.removeprefix("gs://raw-historic-data/")
    bucket = storage.Client(project=PROJECT).bucket("raw-historic-data")
    current = bucket.get_blob(name, timeout=30)
    if current is None or int(current.generation) != request.generation or not current.temporary_hold or current.size > MAX_INPUT:
        raise ValueError("Beam requires the current generation to be held and at most 16 MiB")
    data = bucket.blob(name, generation=request.generation).download_as_bytes(if_generation_match=request.generation, timeout=30)
    if hashlib.sha256(data).hexdigest() != request.sha256:
        raise ValueError("Historical source hash mismatch")
    text = data.decode("utf-8-sig")
    if len(text.splitlines()) > 100000:
        raise ValueError("Historical input exceeds record/metadata line budget")
    if request.feed == "cocoa":
        header = next(csv.reader(io.StringIO(text)))
        if len(header) != 5 or header[0] != "Date" or header[4] != "ICCO daily price (Euro/tonne)":
            raise ValueError("Expected publisher-dated ICCO EUR/tonne CSV")
    elif request.feed == "weather":
        if "YEAR,DOY,PRECTOTCORR,GWETROOT" not in text.splitlines():
            raise ValueError("Expected NASA POWER daily CSV fields")
    else:
        body = json.loads(text)
        if (body.get("series") != "RBRTE" or body.get("units") != "$/BBL"
                or body.get("acquisition_interface") != "published-table"):
            raise ValueError("Expected EIA published-table Brent USD/barrel history")


def fingerprints(feed):
    folder = Path(__file__).resolve().parent
    return {name: hashlib.sha256((folder / name).read_bytes()).hexdigest()
            for name in (SCRIPTS[feed], "backfill_contract.py", "backfill_recovery.py", "setup.py")}


def verify_destination(request):
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT, location="europe-west3")
    dataset = client.get_dataset(request.dataset)
    if (dataset.labels.get("purpose") != "historical-backfill" or dataset.labels.get("token") != request.token
            or dataset.location.lower() != "europe-west3" or dataset.default_table_expiration_ms != 172800000):
        raise ValueError("Expected owned regional candidate dataset with two-day table expiry")
    for table in TABLES[request.feed]:
        try:
            existing = client.get_table(request.dataset + "." + table)
        except NotFound:
            continue
        if existing.num_rows or existing.table_type != "TABLE":
            raise ValueError("Refusing to reuse nonempty or nonphysical backfill outputs")


def configure(feed, argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--input-generation", required=True, type=int)
    parser.add_argument("--input-sha256", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-dataset", required=True)
    parser.add_argument("--manifest-output", required=True)
    parser.add_argument("--source-acquired-at")
    parser.add_argument("--allow-dataflow", action="store_true")
    args, beam_args = parser.parse_known_args(argv)
    request = BackfillRequest(feed, args.input, args.input_generation, args.input_sha256,
                              args.start_date, args.end_date, args.output_dataset, args.manifest_output, args.source_acquired_at)
    if Path(request.manifest_output).exists():
        raise ValueError("Refusing to overwrite a completed backfill manifest")
    from apache_beam.options.pipeline_options import DirectOptions, GoogleCloudOptions, PipelineOptions, StandardOptions, WorkerOptions

    options = PipelineOptions(beam_args)
    standard = options.view_as(StandardOptions)
    if standard.streaming:
        raise ValueError("Historical backfill must be a batch job")
    runner = standard.runner or "DirectRunner"
    if runner not in {"DirectRunner", "DataflowRunner"} or (runner == "DataflowRunner" and not args.allow_dataflow):
        raise ValueError("Use DirectRunner; Dataflow requires explicit --allow-dataflow")
    standard.runner = runner
    cloud = options.view_as(GoogleCloudOptions)
    if cloud.project not in {None, PROJECT} or cloud.region not in {None, "europe-west3"}:
        raise ValueError("Unexpected backfill project/region")
    cloud.project, cloud.region = PROJECT, "europe-west3"
    cloud.temp_location = f"gs://cocoa-prices-temp-for-bq/backfill/{request.token}/{feed}/"
    cloud.staging_location = cloud.temp_location + "staging/"
    if runner == "DirectRunner":
        options.view_as(DirectOptions).direct_num_workers = 1
    else:
        worker = options.view_as(WorkerOptions)
        worker.num_workers = worker.max_num_workers = 1
        worker.machine_type, worker.disk_size_gb, worker.autoscaling_algorithm = "e2-standard-2", 30, "NONE"
        cloud.dataflow_service_options = ["max_workflow_runtime_walltime_seconds=1800"]
    verify_source(request)
    verify_destination(request)
    return replace(request, transform_hashes=fingerprints(feed)), options


def in_interval(record, request):
    observed = record.get("Date" if request.feed == "cocoa" else "date")
    return observed is not None and request.start <= observed <= request.end


def persist_invalid(records, request):
    """One compressed immutable object; no raw-bucket rename/delete permission."""
    if not records:
        return None
    payload = "\n".join(sorted(json.dumps(row, sort_keys=True, allow_nan=False) for row in records)).encode()
    if len(payload) > 64 * 1024**2:
        raise ValueError("Historical rejection evidence exceeds 64 MiB budget")
    data = gzip.compress(payload, mtime=0)
    digest = hashlib.sha256(data).hexdigest()
    from google.api_core.exceptions import PreconditionFailed
    from google.cloud import storage

    blob = storage.Client(project=PROJECT).bucket("raw-historic-data").blob(request.quarantine_prefix + digest + ".jsonl.gz")
    blob.metadata = {"sha256": digest, "source_sha256": request.sha256}
    try:
        blob.upload_from_string(data, if_generation_match=0, content_type="application/gzip", timeout=30)
    except PreconditionFailed:
        blob.reload(timeout=30)
        if (blob.metadata or {}).get("sha256") != digest:
            raise ValueError("Historical rejection archive identity collision") from None
    return blob.name


def finish(request, result):
    if result.state != "DONE":
        raise RuntimeError("Backfill did not complete; retain isolated outputs and start a new run")
    if request.transform_hashes != fingerprints(request.feed):
        raise RuntimeError("Backfill code changed during execution; do not approve these candidates")
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT, location="europe-west3")
    counts = {}
    for kind in ("valid", "invalid"):
        try:
            counts[kind] = client.get_table(request.table(kind == "invalid").replace(":", ".", 1)).num_rows
        except NotFound:
            counts[kind] = 0
    output = Path(request.manifest_output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump({"version": 1, "status": "completed", "request": asdict(request), "counts": counts,
                   "quarantine_uri_prefix": "gs://raw-historic-data/" + request.quarantine_prefix}, stream, sort_keys=True)
