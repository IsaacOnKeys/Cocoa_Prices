"""Run the actual historical graphs on tiny fixtures with explicit IO isolation."""

import argparse
import hashlib
import json
import re
import runpy
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
PROJECT = "cocoa-prices-430315"
FEEDS = {
    "cocoa": ("clean_cocoa_prices.py", "cocoa_prices.csv", "snapshots/2025-10-02/icco/Daily_Prices_Home.csv", "cocoa", "invalid_cocoa"),
    "oil": ("clean_oil_eia.py", "brent_oil_eia.json", "snapshots/2026-10-27/eia/brent_oil_eia.json", "brent_prices", "invalid_brent_prices"),
    "weather": ("clean_precipitation.py", "power_daily.csv", "snapshots/2025-10-02/nasa_power/POWER_Point_Daily.csv", "precipitation", "invalid_precipitation"),
}
EXPECTED = {
    "cocoa": [{"Date": d, "Euro_Price": v} for d, v in [
        ("2025-10-01", 5563.35), ("2025-09-30", 5595.40), ("2025-09-29", 5765.64),
        ("2025-09-26", 5736.05), ("2025-09-25", 5754.78)]],
    "oil": [{"date": d, "brent_price_eu": v} for d, v in [
        ("2014-01-02", 107.94), ("2014-01-03", 106.57), ("2014-01-06", 106.71),
        ("2014-01-07", 107.01), ("2014-01-08", 107.42)]],
    "weather": [{"date": d, "precipitation": p, "soil_moisture": m} for d, p, m in [
        ("2014-01-01", 2.44, 0.76), ("2014-01-02", 3.02, 0.76), ("2014-01-03", 2.04, 0.76),
        ("2016-12-31", 0.0, 0.82), ("2025-09-29", 0.49, 0.41)]],
}


def checked_namespace(dataset):
    match = re.fullmatch(rf"{re.escape(PROJECT)}\.runtime_validation_(\d{{8}}_[0-9a-f]{{8}})", dataset)
    if not match:
        raise ValueError("Only the dedicated runtime validation namespace is allowed")
    return match[1]


def mapped_table(original, feed, dataset):
    allowed = {f"{PROJECT}:cocoa_related.{table}" for table in FEEDS[feed][3:]}
    candidate = re.fullmatch(re.escape(PROJECT) + r":backfill_[0-9]{8}_[a-f0-9]{8}\.([A-Za-z_]+)", original or "")
    if original not in allowed and (not candidate or candidate[1] not in FEEDS[feed][3:]):
        raise ValueError("Unexpected sink in the historical graph")
    checked_namespace(dataset)
    return dataset.replace(".", ":", 1) + "." + original.rsplit(".", 1)[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scripts-dir", type=Path, default=ROOT / "pipelines/backfill")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cloud-dataset", help="Optional newly created, labeled isolated dataset; never production")
    parser.add_argument('--recovery-cases', action='store_true', help='Include malformed, missing and nonfinite inputs; prove append-only cloud invalid output')
    parser.add_argument('--revision-cases', action='store_true', help='Collapse identical facts and reject conflicting unordered revisions')
    args = parser.parse_args()
    token = checked_namespace(args.cloud_dataset) if args.cloud_dataset else None
    output = args.output_dir.resolve()
    if output.exists() and any(output.iterdir()):
        raise ValueError("Refusing to overwrite an existing runtime receipt")
    output.mkdir(parents=True, exist_ok=True)
    import apache_beam as beam
    sys.path.insert(0, str(args.scripts_dir.resolve()))
    import backfill_recovery
    import backfill_contract

    if (args.scripts_dir/'backfill_recovery.py').read_bytes() != (ROOT/'pipelines/backfill/backfill_recovery.py').read_bytes():
        raise ValueError('Packaged recovery helper differs from the checkout')

    client = None
    if token:
        from google.cloud import bigquery

        client = bigquery.Client(project=PROJECT, location="europe-west3")
        dataset = client.get_dataset(args.cloud_dataset)
        if dataset.labels.get("purpose") != "python311-validation" or dataset.labels.get("token") != token:
            raise ValueError("Dataset ownership labels do not match")
        if list(client.list_tables(args.cloud_dataset)):
            raise ValueError("Runtime validation requires a fresh empty dataset")
    original_read, original_write = beam.io.ReadFromText, beam.io.WriteToBigQuery
    summary = {}
    for feed, (script, sample, archive, valid_table, invalid_table) in FEEDS.items():
        source = args.scripts_dir / script
        if source.read_bytes() != (ROOT / "pipelines/backfill" / script).read_bytes():
            raise ValueError("Packaged cleaner differs from the checkout")
        fixture = output / sample
        data = (ROOT / "data/samples" / sample).read_bytes()
        if feed == "oil":
            payload = json.loads(data)
            payload["response"]["data"].append({"period": "not-a-date", "value": "NA", "series": "RBRTE", "units": "$/BBL"})
            if args.recovery_cases:
                payload['response']['data'].append({'period': '2026-10-20', 'value': 'PRIVATE malformed number', 'series': 'RBRTE', 'units': '$/BBL'})
            if args.revision_cases:
                payload['response']['data'].extend([dict(payload['response']['data'][0]),
                    {'period':'2026-10-20','value':'70.0','series':'RBRTE','units':'$/BBL'},
                    {'period':'2026-10-20','value':'71.0','series':'RBRTE','units':'$/BBL'}])
            fixture.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        else:
            bad = b'"not-a-date","0","0","0","bad-price"' if feed == "cocoa" else b"2013,365,1.0,0.5"
            extra = b''
            if args.recovery_cases:
                extra = (b'PRIVATE malformed CSV\n"20/10/2026","0","0","0","nan"\n' if feed == 'cocoa'
                         else b'2014,1,-999,0.5\nPRIVATE malformed CSV\n2014,367,1.0,0.5\n')
            if args.revision_cases:
                extra += (data.splitlines()[1]+b'\n"20/10/2026","0","0","0","9000"\n"20/10/2026","0","0","0","9001"\n'
                          if feed=='cocoa' else b'2014,1,2.44,0.76\n2024,20,1.0,0.4\n2024,20,2.0,0.4\n')
            fixture.write_bytes(data.rstrip(b"\r\n") + b"\n" + bad + b"\n" + extra)
        observed = set()

        def reader(path, *positional, **kwargs):
            if path != fixture_uri:
                raise ValueError("Unexpected source in the historical graph")
            return original_read(str(fixture), *positional, **kwargs)

        class LocalSink(beam.PTransform):
            def __init__(self, table):
                self.path = str(output / table)

            def expand(self, rows):
                return (rows
                        | beam.Map(lambda row: __import__("json").dumps(row, sort_keys=True, allow_nan=False))
                        | beam.io.WriteToText(self.path, file_name_suffix=".jsonl", num_shards=1, shard_name_template=""))

        def writer(*positional, **kwargs):
            original = kwargs.get("table", positional[0] if positional else None)
            # Validate every target even in local mode; never call cloud IO there.
            target = mapped_table(original, feed, args.cloud_dataset or f"{PROJECT}.runtime_validation_20000101_00000000")
            name = original.rsplit(".", 1)[1]
            observed.add(name)
            if name == invalid_table and kwargs['write_disposition'] != beam.io.BigQueryDisposition.WRITE_APPEND:
                raise ValueError('Invalid historical evidence must be append-only')
            if name == valid_table and kwargs['write_disposition'] != beam.io.BigQueryDisposition.WRITE_EMPTY:
                raise ValueError('Historical candidates must never overwrite existing facts')
            if token:
                kwargs["table"] = target
                kwargs["custom_gcs_temp_location"] = f"gs://cocoa-prices-temp-for-bq/runtime-validation/{token}/"
                return original_write(**kwargs)
            return LocalSink(name)

        fixture_uri = f"gs://raw-historic-data/engineering-validation/{token or '20000101_00000000'}/{sample}"
        argv = [str(source), "--runner=DirectRunner", "--direct_num_workers=1", "--direct_running_mode=in_memory",
                "--input", fixture_uri, "--input-generation", "1", "--input-sha256", hashlib.sha256(fixture.read_bytes()).hexdigest(),
                "--start-date", "2014-01-01", "--end-date", "2026-10-20",
                "--output-dataset", f"{PROJECT}.backfill_{token or '20000101_00000000'}",
                "--manifest-output", str(output / (feed + "-manifest.json"))]
        iterations = 1  # New candidate writes are WRITE_EMPTY, never destructive reruns.
        contract = backfill_recovery.SOURCES[feed]

        def archived(records, request):
            import gzip
            destination = output / (feed + '-quarantine.jsonl.gz')
            destination.write_bytes(gzip.compress('\n'.join(json.dumps(row, sort_keys=True) for row in records).encode()))
            return str(destination)

        with patch.object(beam.io, "ReadFromText", reader), patch.object(beam.io, "WriteToBigQuery", writer), patch.object(sys, "argv", argv), patch.dict(
                backfill_recovery.SOURCES, {feed: (str(fixture), contract[1], contract[2])}), \
                patch.object(backfill_contract, 'verify_source', lambda request: None), \
                patch.object(backfill_contract, 'verify_destination', lambda request: None), \
                patch.object(backfill_contract, 'persist_invalid', archived), \
                patch.object(backfill_contract, 'finish', lambda request, result: None):
            for _ in range(iterations):
                runpy.run_path(str(source), run_name="__main__")
        if observed != {valid_table, invalid_table}:
            raise RuntimeError("The expected valid/invalid sinks were not both exercised")
        results = {}
        for table in (valid_table, invalid_table):
            if client:
                records = [json.loads(r["row_json"]) for r in client.query(
                    f"SELECT TO_JSON_STRING(T) row_json FROM `{args.cloud_dataset}.{table}` T").result()]
            else:
                records = [json.loads(line) for line in (output / (table + ".jsonl")).read_text(encoding="utf-8").splitlines() if line]
            results[table] = records
        valid, invalid = results[valid_table], results[invalid_table]
        def canonical(rows):
            return sorted(rows, key=lambda r: r.get("date", r.get("Date")))
        expected_invalid = (({'cocoa': 3, 'oil': 2, 'weather': 4}[feed] if args.recovery_cases else 1)
                            +(3 if args.revision_cases else 0)) * iterations
        if canonical(valid) != canonical(EXPECTED[feed]) or len(invalid) != expected_invalid or not invalid[0].get("Errors"):
            raise RuntimeError(f"Unexpected {feed} fixture output; inspect isolated evidence")
        for row in invalid:
            evidence = json.loads(row['Errors'])
            if (evidence['version'] != 1 or evidence['source_uri'] != fixture_uri
                    or hashlib.sha256(evidence['raw_payload'].encode()).hexdigest() != evidence['input_sha256']):
                raise RuntimeError('Historical failure evidence is incomplete')
        if args.revision_cases:
            reasons=[json.loads(row['Errors'])['errors'] for row in invalid]
            assert reasons.count('Conflicting source revisions for date')==2*iterations
            assert reasons.count('Duplicate identical source fact')==iterations
        summary[feed] = {"valid": len(valid), "invalid": len(invalid), "graph_executions": iterations,
                         "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(), 'failure_evidence_verified': True,
                         'duplicate_policy_verified':args.revision_cases}
    result = {"python": sys.version.split()[0], "beam": beam.__version__, "cloud_dataset": args.cloud_dataset,
              "production_writes": 0, "feeds": summary}
    (output / "receipt.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
