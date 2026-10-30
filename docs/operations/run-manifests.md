# Run and release manifests

Checklist item 19, branch `codex/durable-run-manifests`.

The existing release registry remains authoritative for input cutoff, frozen
acquisitions, source coverage, baseline/output counts and fingerprints, quality
results, procedure identity, model, publication time and release outcome.
The additive `analytics_delivery.run_manifests` table captures that registry
entry together with the orchestration context, including runs rejected before
a release registry entry exists.

The existing `summary` task records:

- Run/release IDs, business date, actual run start, upstream task states and
  success/failure outcome.
- Exact accepted receipt objects, generations, digests, acquisition IDs,
  observation inventories, source ages and counts.
- Installed DAG SHA256, pinned procedure SHA256, wire contract version 1,
  effective non-secret configuration and its SHA256.
- Actual release BigQuery job IDs, completion/error reason and processed/billed
  bytes and slot milliseconds, obtained through the job metadata API.
- The release registry row as JSON, including cutoff, quality checks,
  model identity and publication time; NULL if publication was never attempted
  or failed before its input snapshot was recorded.

The manifest records the declared wire contract version, rather than inventing
an embedded schema identifier in schemaless Avro. Configuration excludes API
keys, connection credentials, endpoint tokens and HTTP/SDK error bodies.
Acquisition IDs connect producer/consumer logs to receipt references; the
manifest task logs its manifest ID, release ID and BigQuery write-job ID.

## Failure and retry behavior

`summary` waits for all three feed gates and the release task, using `all_done`.
It saves failed runs, then explicitly fails if any upstream task was not
successful. It cannot hide a producer, sensor or publication failure behind a
successful terminal task. If manifest persistence itself fails, the task fails
and retries once; normal Airflow retry/recovery remains available. An accepted
release is not rolled back solely because this subsequent audit write failed.

Each summary attempt has an immutable manifest ID `<release_sha256>_<try>` and
deterministic job ID `cocoa_manifest_<manifest_id>`. An uncertain write reattaches
to that job and verifies its request instead of inserting it again. A later
Airflow attempt appends a new snapshot and preserves earlier attempts.

## Storage, cost and inspection

There is no new service, bucket, dataset, IAM grant or container dependency.
The small table uses the existing delivery dataset, date partitions, release-ID
clustering and 400-day partition expiry. Each write is capped at 20 MiB billed:
BigQuery applies a 10 MiB minimum to the registry lookup and target insert even
when their actual data is much smaller. This limit is per job, not a monthly
monetary cap. Job metadata reads do not rerun the pipeline or models.

The owner's budget targets **under EUR 5/month**, with a **EUR 10/month maximum**,
including cloud validation. Actual account spend has not been verified; these
workload bounds alone do not prove that the whole project meets the budget.

Filter by business date when inspecting the table:

```sql
SELECT manifest_id,outcome,recorded_at,manifest
FROM `cocoa-prices-430315.analytics_delivery.run_manifests`
WHERE business_date=DATE '2026-10-30'
  AND release_id='<release SHA256>'
ORDER BY recorded_at DESC;
```

The manifest's `release.quality_results.artifacts` contains published artifact
counts/fingerprints. `release.source_receipts` and `accepted_sources` point back
to generation-pinned inventories; `analytics_delivery.frozen_inputs` retains
the corresponding observations. Existing releases before this feature retain
their original registry records; missing old orchestration metadata is not
fabricated or retroactively labeled as captured at execution time.

## Deployment and validation

Apply `run_manifest_migration` from the existing SQL renderer before installing
the updated DAG at `/opt/airflow/dags/cocoa_daily_dag.py`. The publication procedure,
its hash pin, source functions, Beam containers and serving data are unchanged.
The prior DAG file is retained on the VM for recovery. A rollback can restore
that file while retaining the additive table and its existing records.

Python 3.11 validation: 11 focused unit tests and changed-file Ruff passed.
BigQuery created the table with zero bytes billed and accepted a dry-run of
the actual writer SQL. A real, explicitly labeled failed engineering run
demonstrated insert, registry-absent handling and failure propagation; a bounded
read verified the saved JSON, and the exact fixture row was deleted. No source
acquisition, publication, model training or full pipeline rerun was performed.
Evidence is in `.build/durable-run-manifests/metadata-validation.json` and
`.build/durable-run-manifests/native-io.json`. Scheduled-run evidence begins
after DAG installation; it is distinct from this focused feature validation.

The DAG was installed on 2026-10-30 with SHA256
`d45ba1dea814ef7241b52569c7487a7c2966d3e32504dc86cf935e591bbe2d11`.
The existing scheduler container was stopped during this maintenance session;
its native DAG import was deferred rather than starting additional services.
The prior file is `/opt/airflow/releases/run-manifests-20261030/cocoa_daily_dag.py.before`.
The VM was restored to `TERMINATED`; `.build/durable-run-manifests/vm-deployment.json`
records the maintenance interval. No full pipeline was rerun. Normal startup
and scheduled manifest collection remain operational evidence to observe,
including the existing startup/scheduling work in checklist item 27.
