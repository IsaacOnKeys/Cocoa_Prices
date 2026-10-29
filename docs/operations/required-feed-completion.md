# Required-feed completion and source eligibility

Checklist item 13, branch `codex/required-feed-completion`. Deployed and verified on 2026-10-29. All credential-free and native isolated
checks passed. A controlled live run rejected the actual eight-day NASA gap
before SQL; the six October 28 serving tables and model remain unchanged.
The DAG is now unpaused with the required gate enforcing the existing limit.

All three feeds are mandatory under the [service contract](service-contract.md).
The previous increasing-timestamp/one-row predicate, soft-fail sensor timeout and
optional ONE_SUCCESS merge rule could publish with a missing feed. The item 9
cycle also showed why a complete filled calendar cannot establish weather
freshness: NASA exposed explicit missing values October 22-29 while the existing
SQL checks passed. Its last valid date was October 21, eight days old on October
29, above the seven-day limit. The [withholding receipt](acquisition-coverage.md)
records the preserved candidate and verified October 28 restoration.

## Required evidence before SQL

The existing capture_baseline task now captures the intended run/business date
and acquisition window. A scheduled business date comes from data_interval_end,
not the interval's previous logical date. Stored instants remain UTC; the normal
window begins at 18:30 Europe/Berlin on a weekday. A scheduled run starting on a
different business date must use a new recovery run. An explicit boolean
engineering_validation permits a controlled manual run outside the service
window; it never relaxes source freshness, completeness or provenance.

Each trigger returns its invocation-start instant. The sensor computes the exact
acquisition identity from DAG/run/feed and the configured source scope, matching
the existing producer identity v1. The pinned Functions Framework 3.8.3 Pub/Sub
conversion supplies service/name/type scope; HTTP Cocoa uses null scope. This
was checked against all three actual item 9 acquisitions and real HTTP/Gen2
adapter fixtures. Provider/framework/source-scope changes require compatibility
validation. No producer, consumer, Avro, Beam, source topic or IAM change is needed.

For each required feed the gate:

1. Reads only the current acquisition's immutable receipt prefix. It verifies
   content SHA-256, generation, source/version/status, aware acquisition time,
   actual requested window, unique dated revisions and the complete partition
   between observations and dates not exposed by the successful response.
2. Ignores completions preceding the current trigger attempt. Chooses the latest
   completed attempt; conflicting acquisition instants fail explicitly. It does
   not select an older eligible attempt to conceal newer missing source data.
3. Requires a valid observation for every mandatory measurement within its
   accepted age: Cocoa seven days, EIA fourteen, and each NASA measurement seven.
   Source-explicit missing values remain missing. A lagged but available valid
   quote can qualify; today's fabricated quote or an old replay cannot.
4. Queries staging with parameterized acquisition ID and exact producer acquisition
   time. Aggregates physical duplicates by date/revision. Every declared revision
   must have arrived, provenance must be valid, and null/missingness must match
   the source inventory. Contradictions fail; an incomplete batch keeps waiting.
5. Stores the accepted receipt generation/hash, attempt/window, logical counts
   and per-measurement dates/ages in XCom for subsequent run/release tracing.

No old baseline row count, latest ingestion timestamp or unrelated delivery can
substitute for this evidence. NASA must account for every requested calendar
key; Cocoa/EIA source absences are not invented market-closure explanations.
The sensor validates acquisition eligibility, not final report acceptance.

## Failure and request bounds

All three source sensors and the existing settle task require ALL_SUCCESS.
Sensors use soft_fail=False and no task retry that could restart their overall
timeout. Transient receipt/query errors return to polling within that deadline;
missing receipt/rows eventually fail rather than skip. An invalid receipt,
contradictory sink or exceeded source age fails permanently and immediately.
Normal completion uses a ten-minute maximum, polling every 1-60 seconds. The
stored completion timeout was normalized to ten minutes during preflight.

One poke has a cooperative 45-second IO budget and a one-minute task execution
limit. Storage requests wait at most five seconds, SDK retries are disabled,
receipt attempts are capped at 32 and each object at 256 KiB. Parameterized
standard-SQL queries cap bytes at 1 GiB and polling at the remaining budget.
Timeouts bound requests cooperatively; backend reads can finish after a caller
timeout. Task timeout still fails the required gate and prevents SQL.

The old PROGRESS_REQUIRE_ALL/PROGRESS_MIN_NEW_ROWS switches no longer weaken the
mandatory contract. The existing TimeDeltaSensor remains for item 14, but is
not used as evidence that a batch is complete. Generic candidate snapshots,
source cutoffs/revisions, recurring quality and all-or-nothing forecast failure
publication remain items 15, 17 and 19-20. This gate does not replace those controls.

## Validation evidence

All 384 Python 3.11 tests pass; Ruff, 53 Python and five shell syntax checks
and pip check pass. Tests cover real adapter identity,
Berlin daylight saving, interval-end business dates, latest versus prior attempts,
missing and invalid receipts, checksum/size/attempt bounds, source-age boundaries,
each weather measurement, duplicate/late rows, invalid provenance and sink
missingness contradictions. Transient IO never exposes SDK exception canaries.

Native Python 3.11.10 / Airflow 2.10.2 / Google provider 10.22.0 parsed ten tasks
and verified required dependencies, hard timeouts, trigger retries and all three
actual identity-v1 samples. Candidate SHA-256:
`59551d0f1fdbf33dd83cc7d805dd420ebf412e619a98ebc26cbcbfd193d877b8`.
The staged file location was checked explicitly to prevent DagBag fallback to
an installed older DAG; candidate bytes were transferred with compression to
fit Windows command limits. The identical candidate is now installed at the stable live path.

The [isolated validation helper](../../scripts/diagnostics/validate_required_feeds.py)
created bucket/dataset namespace `20261029_2039893f`. The actual candidate gate ran
inside the installed Airflow container using its real GCS/BQ hooks and service
identity. All ready feeds passed; a missing weather receipt and an incomplete
31-row batch waited; a single eight-day-old weather measurement failed; prior
attempts did not qualify. Adding only the final delayed row changed that case
to all three feeds passed. No production data was written, and both disposable
resources were removed. Existing Airflow project viewer/job permissions sufficed;
no temporary or production IAM grant was introduced.

No hosted CI, rendered BI, achieved SLO, generic release atomicity or improved
forecast accuracy is claimed. The current NASA delay must remain visible and
blocked; changing the seven-day limit would require the owner's acceptance.


## Live deployment and rejection proof

Implementation `83c9638`. Only the DAG and its non-secret completion-timeout
configuration changed; all four function source archives were re-read and matched
the unchanged Python 3.11 packages byte-for-byte. No producer/consumer, schema,
Beam, topic, SQL procedure, model or IAM deployment was needed. The file was
replaced atomically with UID 1001/GID 0/mode 0644, then parsed in the actual
container. Scheduler adoption was verified through Airflow 2.10.2's
SerializedDagModel record, using the required ALL_SUCCESS rule and one-minute
sensor execution limits. An initial verification helper used an unsupported
serialized-DAG API; that helper was corrected while scheduling remained paused.

Run `required_feeds__83c9638__20261029` executed 11:11:02-11:11:53 UTC
(12:11 Berlin), explicitly marked engineering validation. Capture and all three
producer triggers succeeded. Cocoa and EIA completion gates passed. Weather
failed permanently because its latest valid precipitation remained eight days
old, above seven. The settle, BigQuery pipeline and summary tasks became
upstream_failed; the DAG correctly became failed. No pipeline job was submitted.
All six serving row counts/fingerprints and the model etag are unchanged, and the
SQL procedure hash is unchanged. The October 28 outputs remain visible; this
is a verified rejection, not a failed implementation or a current release claim.

The DAG is unpaused and idle, with its next normal opportunity October 29 at
18:00 UTC. A still-stale source will fail the gate rather than refresh outputs.
The VM is temporarily running for consecutive item 14 maintenance; legacy daily
Beam units remain disabled/inactive. Both Terraform roots have no drift and
inventory remains 112 managed resources. No sustained delivery or alert receipt
has been demonstrated.

Held DAG source/rollback objects and the complete manifest are under
`gs://raw-historic-data/operations/2026-10-29/required-feeds-83c9638/`.
The before-source SHA-256 is
`fe1f8863e451f8fae09db5c218daebca0debe00c4833d6b1dc7b2da71633e0ba`;
the installed source SHA-256 is recorded above. Exact generations, all isolated
outcomes/removal, effective native policy, controlled task states/rejection,
source proof references and unchanged serving state are retained in the manifest.
Generation `1793272773145000`, SHA-256
`1fc27cfd5835f11e8bac5af591e598d15608657e68d35cba673d9867b10c1d5d`.

For recovery, pause/confirm no active run before changing the DAG. Preserve the
accepted source-age policy and October 28 serving data; do not resume the older
permissive DAG merely to obtain a green run. The exact prior file is root-only
at `/opt/airflow/releases/required-feeds-20261029/cocoa_daily.before.py` and held
in GCS. Restore a compatible corrected gate and validate native parsing, complete
and rejected source cases, serving bytes and scheduler adoption before unpausing.
