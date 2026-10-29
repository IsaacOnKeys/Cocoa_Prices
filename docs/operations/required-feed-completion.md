# Required-feed completion and source eligibility

Checklist item 13, branch `codex/required-feed-completion`. Implementation,
credential-free checks and native isolated GCS/BigQuery validation pass. Live DAG
installation and the controlled production rejection check remain pending.
Serving outputs remain the restored October 28 release; the DAG is paused.

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
fit Windows command limits. The live DAG has not yet been replaced.

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
