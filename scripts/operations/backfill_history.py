"""Create isolated Beam runs, preview closed-history repairs and queue reviewed plans."""

import argparse
from datetime import date, datetime, timedelta, timezone
import hashlib
import importlib
import json
import math
from pathlib import Path
import re
import shlex
import sys
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "pipelines/backfill"))
CONTRACT = importlib.import_module("backfill_contract")
PROJECT = CONTRACT.PROJECT
FIELDS = ("price", "precipitation", "soil_moisture")
FEEDS = {"cocoa": ("clean_cocoa_prices.py", "cocoa", "Date", {"price": "Euro_Price"}),
         "oil": ("clean_oil_eia.py", "brent_prices", "date", {"price": "brent_price_eu"}),
         "weather": ("clean_precipitation.py", "precipitation", "date",
                     {"precipitation": "precipitation", "soil_moisture": "soil_moisture"})}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def identity(plan):
    return hashlib.sha256(canonical(plan).encode()).hexdigest()


def context(token=None):
    if token is None:
        return {"delivery": "analytics_delivery", "curated": "cocoa_related"}
    if not re.fullmatch(r"[0-9]{8}_[a-f0-9]{8}", token):
        raise ValueError("Expected an isolated validation token")
    return {"delivery": "backfill_validation_" + token, "curated": "backfill_baseline_" + token}


def checked_context(value):
    delivery = value.get("delivery", "")
    expected = context(delivery.removeprefix("backfill_validation_")) if delivery.startswith("backfill_validation_") else context()
    if value != expected:
        raise ValueError("Unexpected or mixed historical repair target")
    return value


def normalized(feed, row):
    result = {key: None for key in FIELDS}
    for key in FIELDS:
        value = row.get(key)
        if value is None:
            continue
        if key not in FEEDS[feed][3] or isinstance(value, bool):
            raise ValueError("Unexpected historical measurement")
        value = float(value)
        if (not math.isfinite(value) or (key == "price" and value <= 0)
                or (key == "precipitation" and value < 0) or (key == "soil_moisture" and not 0 <= value <= 1)):
            raise ValueError("Historical measurement fails its domain")
        result[key] = value
    return result


def build_plan(feed, candidates, baseline, start, end, source, target, *, today=None, prepared_at=None):
    today = today or date.today()
    lower, upper = date.fromisoformat(start), date.fromisoformat(end)
    checked_context(target)
    if (feed not in FEEDS or lower.isoformat() != start or upper.isoformat() != end
            or not date(2014, 1, 1) <= lower <= upper < today - timedelta(days=90)
            or (upper - lower).days > 365):
        raise ValueError("Repairs require a closed interval of at most 366 days and a fixed target pair")
    before, proposed = {}, {}
    for rows, destination in ((baseline, before), (candidates, proposed)):
        for row in rows:
            observed = row["date"].isoformat() if isinstance(row["date"], date) else row["date"]
            if date.fromisoformat(observed).isoformat() != observed or not start <= observed <= end or observed in destination:
                raise ValueError("Duplicate or out-of-interval historical date")
            destination[observed] = normalized(feed, row)
    if not proposed:
        raise ValueError("No valid historical candidates to review")
    rows, counts = [], {"inserted_dates": 0, "changed_dates": 0, "unchanged_dates": 0, "preserved_missing_measurements": 0}
    for observed, values in sorted(proposed.items()):
        previous = before.get(observed, dict.fromkeys(FIELDS))
        effective = {key: values[key] if values[key] is not None else previous[key] for key in FIELDS}
        counts["preserved_missing_measurements"] += sum(values[k] is None and previous[k] is not None for k in FEEDS[feed][3])
        counts["inserted_dates" if observed not in before else "changed_dates" if effective != previous else "unchanged_dates"] += 1
        rows.append({"date": observed, "before_exists": observed in before, "before": previous, "proposed": values})
    return {"version": 1, "feed": feed, "start": start, "end": end, "source": source, "target": target,
            "prepared_at": prepared_at or datetime.now(timezone.utc).isoformat(), "rows": rows, "summary": counts}


def validate_plan(plan):
    rebuilt = build_plan(plan["feed"], [{"date": r["date"], **r["proposed"]} for r in plan["rows"]],
                         [{"date": r["date"], **r["before"]} for r in plan["rows"] if r["before_exists"]],
                         plan["start"], plan["end"], plan["source"], plan["target"], prepared_at=plan["prepared_at"])
    if canonical(rebuilt) != canonical(plan):
        raise ValueError("Historical repair content/summary is inconsistent")
    source = plan["source"]
    return CONTRACT.BackfillRequest(plan["feed"], source["uri"], source["generation"], source["sha256"],
                                     plan["start"], plan["end"], source["candidate_dataset"], "unused", source.get("acquired_at"))


def client():
    from google.cloud import bigquery

    return bigquery.Client(project=PROJECT, location="europe-west3")


def query(sql, parameters=()):
    from google.cloud import bigquery

    config = bigquery.QueryJobConfig(query_parameters=list(parameters), maximum_bytes_billed=256 * 1024**2,
                                     job_timeout_ms=120000, labels={"app": "cocoa-prices", "component": "historical-repair"})
    job = client().query(sql, job_config=config, job_id_prefix="historical_repair_", retry=None, job_retry=None, timeout=20)
    return [dict(row) for row in job.result(timeout=130, job_retry=None)]


def projection(feed):
    _, _, key, columns = FEEDS[feed]
    return key + " AS date," + ",".join(f"{columns.get(field, 'CAST(NULL AS FLOAT64)')} AS {field}" for field in FIELDS)


def create_run(args):
    from google.cloud import bigquery

    if args.output.exists():
        raise ValueError("Use a new completed-run manifest path")
    token = datetime.now(timezone.utc).strftime("%Y%m%d_") + uuid.uuid4().hex[:8]
    dataset_name = PROJECT + ".backfill_" + token
    request = CONTRACT.BackfillRequest(args.feed, args.input, args.input_generation, args.input_sha256,
                                      args.start_date, args.end_date, dataset_name, str(args.output), args.source_acquired_at)
    CONTRACT.verify_source(request)
    dataset = bigquery.Dataset(dataset_name)
    dataset.location = "europe-west3"
    dataset.default_table_expiration_ms = 172800000
    dataset.labels = {"app": "cocoa-prices", "purpose": "historical-backfill", "token": request.token}
    client().create_dataset(dataset)
    command = [sys.executable, str(ROOT / "pipelines/backfill" / FEEDS[args.feed][0]), "--runner=DirectRunner",
               "--input", request.input_uri, "--input-generation", str(request.generation), "--input-sha256", request.sha256,
               "--start-date", request.start, "--end-date", request.end, "--output-dataset", request.dataset,
               "--manifest-output", request.manifest_output]
    if request.acquired_at:
        command.extend(["--source-acquired-at", request.acquired_at])
    print(shlex.join(command))


def prepare(args):
    from google.cloud import bigquery

    if args.output.exists():
        raise ValueError("Use a new repair plan path")
    manifest = json.loads(args.manifest.read_text())
    request = CONTRACT.BackfillRequest(**manifest["request"])
    if manifest.get("status") != "completed" or manifest["counts"]["invalid"] or not manifest["counts"]["valid"]:
        raise ValueError("Require a completed nonempty candidate and zero rejected records; inspect durable quarantine first")
    CONTRACT.verify_source(request)
    dataset = client().get_dataset(request.dataset)
    if dataset.labels.get("purpose") != "historical-backfill" or dataset.labels.get("token") != request.token:
        raise ValueError("Candidate dataset ownership differs from the run")
    table = client().get_table(request.table().replace(":", ".", 1))
    if table.num_rows != manifest["counts"]["valid"]:
        raise ValueError("Candidate changed after its completed manifest")
    start, end = args.start_date or request.start, args.end_date or request.end
    if not request.start <= start <= end <= request.end:
        raise ValueError("Repair must lie within the validated candidate interval")
    target = context(args.validation_token)
    parameters = [bigquery.ScalarQueryParameter("start", "DATE", start), bigquery.ScalarQueryParameter("end", "DATE", end)]
    _, name, key, _ = FEEDS[request.feed]
    selection = projection(request.feed)
    candidates = query(f"SELECT {selection} FROM `{request.dataset}.{name}` WHERE {key} BETWEEN @start AND @end", parameters)
    baseline = query(f"SELECT {selection} FROM `{PROJECT}.{target['curated']}.{name}` WHERE {key} BETWEEN @start AND @end", parameters)
    source = {"uri": request.input_uri, "generation": request.generation, "sha256": request.sha256,
              "acquired_at": request.acquired_at, "candidate_dataset": request.dataset,
              "transform_hashes": request.transform_hashes,
              "candidate_fingerprint": hashlib.sha256(canonical([{**r, "date": r["date"].isoformat()} for r in sorted(candidates, key=lambda r: r["date"])]).encode()).hexdigest()}
    plan = build_plan(request.feed, candidates, baseline, start, end, source, target)
    with args.output.open("x", encoding="utf-8") as stream:
        stream.write(canonical(plan))
    print(canonical({"repair_id": identity(plan), "summary": plan["summary"], "next": "Review the exact plan, then approve its SHA256 with a reason"}))


def approval_sql(plan):
    target, feed = plan["target"], plan["feed"]
    table, key = FEEDS[feed][1:3]
    selection = projection(feed)
    return f"""
BEGIN TRANSACTION;
UPDATE `{PROJECT}.{target['delivery']}.active_release` SET release_id=release_id WHERE singleton=1;
ASSERT (SELECT COUNT(*) FROM `{PROJECT}.{target['delivery']}.active_release` WHERE singleton=1)=1 AS 'Missing release baseline';
ASSERT @id=LOWER(TO_HEX(SHA256(@plan))) AS 'Reviewed repair identity changed';
ASSERT (SELECT COUNT(*) FROM `{PROJECT}.{target['delivery']}.historical_repairs` WHERE repair_id=@id)<=1
 AS 'Duplicate historical repair identity';
ASSERT NOT EXISTS (SELECT 1 FROM `{PROJECT}.{target['delivery']}.historical_repairs` WHERE repair_id=@id
 AND (plan_json IS DISTINCT FROM @plan OR status NOT IN ('approved','applied')))
 AS 'Cancelled/changed plans cannot be reactivated; prepare a new plan';
IF NOT EXISTS (SELECT 1 FROM `{PROJECT}.{target['delivery']}.historical_repairs` WHERE repair_id=@id) THEN
CREATE TEMP TABLE proposed AS SELECT SAFE_CAST(JSON_VALUE(r,'$.date') AS DATE) AS date,
  SAFE_CAST(JSON_VALUE(r,'$.before_exists') AS BOOL) AS before_exists,
  SAFE_CAST(JSON_VALUE(r,'$.before.price') AS FLOAT64) AS price,
  SAFE_CAST(JSON_VALUE(r,'$.before.precipitation') AS FLOAT64) AS precipitation,
  SAFE_CAST(JSON_VALUE(r,'$.before.soil_moisture') AS FLOAT64) AS soil_moisture
FROM UNNEST(JSON_QUERY_ARRAY(@plan,'$.rows')) r;
CREATE TEMP TABLE current_baseline AS SELECT {selection} FROM `{PROJECT}.{target['curated']}.{table}`
WHERE {key} BETWEEN @start AND @end;
ASSERT (SELECT COUNT(*) FROM proposed p LEFT JOIN current_baseline b USING(date)
  WHERE (b.date IS NOT NULL) IS DISTINCT FROM p.before_exists OR p.price IS DISTINCT FROM b.price
    OR p.precipitation IS DISTINCT FROM b.precipitation OR p.soil_moisture IS DISTINCT FROM b.soil_moisture)=0
  AS 'Historical baseline changed; prepare a new plan';
ASSERT (SELECT COUNT(*) FROM `{PROJECT}.{target['delivery']}.historical_repairs`,
 UNNEST(JSON_QUERY_ARRAY(plan_json,'$.rows')) r WHERE status='approved' AND repair_id!=@id
 AND JSON_VALUE(plan_json,'$.feed')=@feed AND SAFE_CAST(JSON_VALUE(r,'$.date') AS DATE) IN (SELECT date FROM proposed))=0
 AS 'An approved repair already covers these source dates';
INSERT `{PROJECT}.{target['delivery']}.historical_repairs`
 (repair_id,plan_json,status,reason,approved_at,approved_by)
 SELECT @id,@plan,'approved',@reason,CURRENT_TIMESTAMP(),SESSION_USER();
END IF;
COMMIT TRANSACTION;
SELECT repair_id,status FROM `{PROJECT}.{target['delivery']}.historical_repairs` WHERE repair_id=@id;
"""


def approve(args):
    from google.cloud import bigquery

    plan = json.loads(args.plan.read_text())
    request = validate_plan(plan)
    if args.approve != identity(plan) or not 1 <= len(args.reason) <= 1024:
        raise ValueError("Approve the exact reviewed SHA256 and provide a bounded reason")
    CONTRACT.verify_source(request)
    parameters = [bigquery.ScalarQueryParameter(name, kind, value) for name, kind, value in
                  [("id", "STRING", identity(plan)), ("plan", "STRING", canonical(plan)), ("reason", "STRING", args.reason),
                   ("feed", "STRING", plan["feed"]), ("start", "DATE", plan["start"]), ("end", "DATE", plan["end"])]]
    print(canonical(query(approval_sql(plan), parameters)))


def cancel(args):
    from google.cloud import bigquery

    target = context(args.validation_token)
    if not re.fullmatch(r"[a-f0-9]{64}", args.repair_id) or not 1 <= len(args.reason) <= 1024:
        raise ValueError("Expected repair identity and cancellation reason")
    sql = f"""BEGIN TRANSACTION;
UPDATE `{PROJECT}.{target['delivery']}.active_release` SET release_id=release_id WHERE singleton=1;
UPDATE `{PROJECT}.{target['delivery']}.historical_repairs` SET status='cancelled',cancelled_at=CURRENT_TIMESTAMP(),
 cancelled_by=SESSION_USER(),cancellation_reason=@reason WHERE repair_id=@id AND status='approved';
COMMIT TRANSACTION;
SELECT repair_id,status FROM `{PROJECT}.{target['delivery']}.historical_repairs` WHERE repair_id=@id;"""
    print(canonical(query(sql, [bigquery.ScalarQueryParameter("id", "STRING", args.repair_id),
                                bigquery.ScalarQueryParameter("reason", "STRING", args.reason)])))


def cleanup(args):
    if not re.fullmatch(PROJECT + r"\.backfill_[0-9]{8}_[a-f0-9]{8}", args.dataset):
        raise ValueError("Only isolated backfill candidate datasets can be removed")
    dataset = client().get_dataset(args.dataset)
    if dataset.labels.get("purpose") != "historical-backfill" or dataset.labels.get("token") != args.dataset.rsplit("_", 1)[1]:
        raise ValueError("Candidate dataset ownership differs")
    client().delete_dataset(dataset, delete_contents=True, timeout=30)
    print("Removed isolated candidates; source/quarantine archives and repair audit were retained.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    create = sub.add_parser("create")
    create.add_argument("--feed", choices=FEEDS, required=True)
    create.add_argument("--input", required=True)
    create.add_argument("--input-generation", type=int, required=True)
    create.add_argument("--input-sha256", required=True)
    create.add_argument("--source-acquired-at")
    for name in ("start-date", "end-date"):
        create.add_argument("--" + name, required=True)
    create.add_argument("--output", type=Path, required=True, help="Future completed-run manifest path")
    prepare_parser = sub.add_parser("prepare")
    prepare_parser.add_argument("--manifest", type=Path, required=True)
    prepare_parser.add_argument("--start-date")
    prepare_parser.add_argument("--end-date")
    prepare_parser.add_argument("--output", type=Path, required=True)
    prepare_parser.add_argument("--validation-token")
    approval = sub.add_parser("approve")
    approval.add_argument("--plan", type=Path, required=True)
    approval.add_argument("--approve", required=True, help="Exact reviewed plan SHA256")
    approval.add_argument("--reason", required=True)
    cancellation = sub.add_parser("cancel")
    cancellation.add_argument("--repair-id", required=True)
    cancellation.add_argument("--reason", required=True)
    cancellation.add_argument("--validation-token")
    removal = sub.add_parser("cleanup")
    removal.add_argument("--dataset", required=True)
    args = parser.parse_args()
    {"create": create_run, "prepare": prepare, "approve": approve, "cancel": cancel, "cleanup": cleanup}[args.action](args)


if __name__ == "__main__":
    main()
