"""Acquire a new EIA archive without printing or persisting the API credential."""

import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def load_producer():
    spec = importlib.util.spec_from_file_location("eia_archive_producer", ROOT / "functions/oil/main.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def read_key(project):
    cli = shutil.which("gcloud.cmd") or shutil.which("gcloud")
    if not cli:
        raise RuntimeError("Google Cloud CLI is required")
    environment = {**os.environ, "CLOUDSDK_CORE_DISABLE_FILE_LOGGING": "true"}
    result = subprocess.run(
        [cli, "secrets", "versions", "access", "latest", "--secret=EIA_OIL_API_KEY",
         f"--project={project}", "--verbosity=error", "--quiet"],
        capture_output=True, text=True, timeout=30, env=environment,
    )
    if result.returncode or not result.stdout.strip():
        raise RuntimeError("EIA secret access failed; CLI output suppressed")
    return result.stdout.strip()


def compare_prices(path, observations):
    """Compare valid dated values; omitted/missing publisher dates stay distinct."""
    previous = json.loads(path.read_text(encoding="utf-8-sig"))
    old = {}
    for row in previous.get("observations", []):
        if row.get("value") not in (None, "", "."):
            old[row["date"]] = float(row["value"])
    new = {r["date"]: r["oil_price"] for r in observations if r["oil_price"] is not None}
    overlap = sorted(old.keys() & new.keys())
    differences = [day for day in overlap if abs(old[day] - new[day]) > 0.000001]
    return {
        "previous_valid_observations": len(old), "compared_observations": len(overlap),
        "different_values": len(differences), "difference_dates": differences[:20],
        "previous_dates_absent_from_new_source": len(old.keys() - new.keys()),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="cocoa-prices-430315")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2014, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat, default=datetime.now(timezone.utc).date())
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--interface", choices=("published-table", "api"), default="published-table")
    parser.add_argument("--compare", type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Output already exists; choose a new archive path")
    producer = load_producer()
    if args.interface == "api":
        observations = producer.fetch_observations(read_key(args.project), args.start, args.end)
        producer.verify_against_published(observations, args.start, args.end)
    else:
        observations = producer.fetch_published_observations(args.start, args.end)
    if not observations or not any(r["oil_price"] is not None for r in observations):
        raise RuntimeError("No valid EIA history returned; archive not written")
    acquired = datetime.now(timezone.utc).isoformat()
    archive = {
        "source": "U.S. Energy Information Administration", "series": producer.SERIES,
        "frequency": "daily", "units": producer.UNITS, "acquired_at": acquired,
        "acquisition_interface": args.interface,
        "request": {"route": producer.URL if args.interface == "api" else producer.PUBLISHED_URL,
                    "start": args.start.isoformat(), "end": args.end.isoformat()},
        "response": {"total": len(observations), "data": [r["source_row"] for r in observations]},
    }
    payload = (json.dumps(archive, indent=2, ensure_ascii=False) + "\n").encode("utf-8")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("xb") as handle:
        handle.write(payload)
    report = {
        "source": "EIA", "series": producer.SERIES, "units": producer.UNITS,
        "acquired_at": acquired, "records": len(observations),
        "coverage_start": observations[0]["date"], "coverage_end": observations[-1]["date"],
        "sha256": hashlib.sha256(payload).hexdigest(), "size_bytes": len(payload),
        "output": str(args.output),
    }
    if args.compare:
        report["comparison"] = compare_prices(args.compare, observations)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # No request URLs, response bodies, credentials or arbitrary error messages.
        raise SystemExit(f"EIA archive acquisition failed ({type(error).__name__}); sensitive details suppressed") from None
