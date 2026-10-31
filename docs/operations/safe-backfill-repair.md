# Safe historical backfill and repair

Checklist item 29, branch `codex/safe-backfill-repair`. Beam builds isolated
candidates; a reviewed plan becomes input to the next normal atomic analytics
release. The backfill client never replaces current serving tables.

## Run boundaries and retained evidence

All three cleaners require an explicit source URI, generation, SHA256, inclusive
date interval, isolated output dataset and a new completed-manifest path. The
source must be the current, held object generation in `raw-historic-data`, at
most 16 MiB / 100,000 text lines. Format checks distinguish publisher-dated
ICCO EUR/tonne CSV, EIA published-table Brent USD/barrel JSON and NASA POWER
daily CSV. Unknown historical acquisition times stay NULL; upload time does not
become supplier availability time.

Only owned `backfill_YYYYMMDD_TOKEN` datasets in the project/region are accepted.
Their labels and two-day default table expiry are checked. Valid outputs use
`WRITE_EMPTY`; reusing nonempty outputs fails. Dates outside the requested
interval do not enter valid candidates. Full-source validation still retains
malformed rows and conflicting/duplicate facts as rejection evidence. A source
with any rejected records cannot be promoted through the normal plan command.

Invalid rows append to the isolated invalid table and are collected into an
immutable gzip object under `raw-historic-data/quarantine/v1/historical-backfill/`.
That existing lifecycle retains evidence for 400 days. Original raw input,
schema, generation/hash, declared acquisition time and transformation-file
hashes remain traceable. Completion requires runner state `DONE` and unchanged
transformation files. Failed/cancelled jobs cannot create a completed manifest.

Temporary file-load objects under `cocoa-prices-temp-for-bq/backfill/` expire
after three days. Candidate cleanup deletes only a namespace with matching
ownership labels; it retains source/quarantine archives and the repair ledger.

## Operator workflow

Use a separate Python 3.11 Beam environment with `pipelines/requirements.txt`.
Run manually with the operator's ADC, or grant a workload identity only the
candidate-dataset and staging-prefix access required for that run. Do not copy
long-lived user credentials onto the VM. Normal daily Airflow does not launch
these backfill jobs.

1. Choose a protected source from `data/historical_manifest.yaml` and create an
   isolated namespace. `create` prints the complete cleaner command; it does
   not start Beam:

   ```bash
   python scripts/operations/backfill_history.py create --feed oil \
     --input gs://raw-historic-data/snapshots/2026-10-27/eia/brent_oil_eia.json \
     --input-generation 1793113993070636 \
     --input-sha256 4adb1bab95f29b3fc3b64bac6cf5b11f0c14c4e7fa9ec6c7b61cf22de09af087 \
     --source-acquired-at 2026-10-27T15:06:14.612085+00:00 \
     --start-date 2014-01-02 --end-date 2014-01-08 \
     --output .build/backfill/oil-run.json
   ```

2. Run the printed command. Inspect its manifest counts and rejection archives.
   Request another namespace after interruption/nonempty output; do not change
   a write disposition to force a rerun over existing facts.
3. Preview a repair of at most 366 days, wholly **older than 90 calendar days**:

   ```bash
   python scripts/operations/backfill_history.py prepare \
     --manifest .build/backfill/oil-run.json --output .build/backfill/oil-plan.json
   ```

   A smaller `--start-date` / `--end-date` pair may select a subinterval of a
   larger completed backfill. Inspect proposed values, exact expected baseline,
   source/code references and inserted/changed/unchanged/missing counts. Recent
   corrections remain in the required daily-source reacquisition path.
4. Queue only the exact reviewed content with an operator reason:

   ```bash
   python scripts/operations/backfill_history.py approve \
     --plan .build/backfill/oil-plan.json --approve REVIEWED_SHA256 \
     --reason "Reviewed publisher history and reconciled the affected interval"
   ```

5. Before application, cancellation remains possible:

   ```bash
   python scripts/operations/backfill_history.py cancel \
     --repair-id REVIEWED_SHA256 --reason "Withdrawn pending source review"
   ```

   After a completed run has been reviewed/materialized, remove its isolated
   tables with `cleanup --dataset cocoa-prices-430315.backfill_YYYYMMDD_TOKEN`.

## Atomic application and recovery

Approval changes only the content-addressed `analytics_delivery.historical_repairs`
ledger. It records `SESSION_USER()`, server approval time and reason, rejects
overlapping pending dates and compares the current source baseline inside a
transaction. Repeating approval returns the existing approved/applied identity;
it does not apply a second correction. Cancelled plans cannot be reactivated.

The next daily release freezes approved repairs into its baseline candidates,
retaining `frozen_inputs.historical_repair_id`. Only named source/date rows
change. Omitted dates do not delete facts, and explicit NULLs preserve known
valid measurements. Current-acquisition precedence remains unchanged. Historical
acquisition/publication times are not invented by linking a repair ID.

Required-feed checks, analytics quality and the reviewed forecast must all pass
before publication. The final transaction rechecks approval status and reviewed
identity, publishes the existing coherent analytics/forecast set and marks the
repair applied in that release. Failure rolls back publication; accepted outputs
stay visible. Cancellation after freeze also blocks publication. A baseline
changed after review requires cancellation/replanning, not an automatic rebase.
Repair history refers to the release in which it applied; a later release
rollback does not automatically reapprove/reapply that repair. Use a new reviewed
plan if correction/reapplication is needed.

## Cost and validation

DirectRunner defaults to one worker. Dataflow requires explicit `--allow-dataflow`:
one e2-standard-2 worker, no autoscaling, 30 GiB disk and a server-side 30-minute
batch runtime option. This optional mode is billable and was not launched for
validation. [Google's service options](https://docs.cloud.google.com/dataflow/docs/reference/service-options)
describe automatic job cancellation. Approval/preview queries have a 256 MiB
cap and two-minute requested timeout; the multi-statement cap accommodates
BigQuery's statement billing minima. These controls complement the existing
budget notifications, not a guaranteed monthly spending cap.

The isolated SQL drill demonstrated correction/insertion, preservation of a
known value when a revision is NULL, unrelated/newer-date preservation,
post-merge rollback, cancellation of a frozen plan, stale-baseline rejection
and idempotent approval after application. It billed 618,659,840 query bytes;
both test datasets were deleted. No production historical plan was approved,
no accepted values were changed, and no model was trained.

Twenty-five focused Python 3.11 checks, changed-file Ruff and Terraform
format/validation passed. Native Python **3.11.18 / Beam 2.65.0** ran the
three changed graphs against small source-format fixtures: five valid rows per
feed and 6/5/7 preserved rejection rows for cocoa/oil/weather. Conflicting
revisions and malformed/missing values were retained; no production sink was
addressed. The code-only image reused the previously validated dependency
layers; the corrected Docker context includes both shared worker modules.
The optional bounded Dataflow profile was checked against the actual SDK
without submitting a job; remote Dataflow execution remains unverified.

The real held EIA archive then produced five isolated candidate rows for
2014-01-02 through 2014-01-08, with zero rejected rows. A repeated destination
check rejected the nonempty output. The temporary candidate dataset, staging
objects and two-hour prefix-scoped validation grant were removed after the
checks. The existing temporary bucket's three-day `backfill/` lifecycle was
verified through its API.

The empty repair ledger/schema and tested procedure were installed on
2026-10-31. Procedure body SHA256:
`974c1ea981a41a82f680505a92913f0df50e5f359ae36e259a54d355e07235e3`.
The matching installed DAG imported successfully, SHA256:
`185e9cf9e2fa83c2c59babe00905c0c773e4d141bfdb0c6b48a9123364915dc8`.
A fresh runtime backup captured that pin. Production remained idle; no
historical repair was approved/applied and no serving values were changed.
The maintenance VM was restored to `TERMINATED`.

Receipts are under `.build/safe-backfill-repair/` and
`/opt/airflow/releases/safe-backfill-repair-20261031/20261031_33517609`.
The SQL drill initially exposed a BigQuery billing-minimum cap and a
constant-SELECT syntax error; both were corrected before the passing drill
and deployment. A full analytics/model pipeline was not rerun for this feature.
