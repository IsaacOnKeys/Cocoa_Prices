# Producer failure signals

Checklist item 7 makes source acquisition failure visible to the actual HTTP and
event adapters. Python 3.11 remains the runtime. The producers explicitly pin
Functions Framework 3.8.3, matching the consumer and credential-free adapter
tests, rather than relying on an untested framework selected at build time.

ICCO returns HTTP 500 on acquisition/publication failure. EIA and NASA POWER
raise sanitized exceptions so their background-event adapters return HTTP 500.
Request, credential, malformed-response and publish failures do not emit a
successful acquisition receipt. An acknowledged prefix of a failed batch can
already exist downstream; retries and reconciliation must account for it.
Trigger retry ownership is unchanged here and belongs to item 8.

NASA POWER validates both parameter maps, nonempty matching date keys, canonical
dates inside the requested window and finite numeric measurements before any
row is published. It no longer silently intersects mismatched parameter dates
or coerces malformed measurements into missing observations. Explicit source
nulls and values at/below -999 remain nullable. Publisher initialization and
partial publication fail the invocation with sanitized messages.

A successful invocation emits one structured acquisition_complete log after
all publish futures acknowledge. Fields include source, status, published count,
actual observation range and aware acquisition time. The receipt does not claim
freshness, new observations or a complete analytical release. ICCO's existing
HTTP response shape is preserved; event handlers return the receipt internally,
while the framework supplies the HTTP acknowledgement.

Expected source lag is distinct from an acquisition failure. An ICCO response
with a valid older exposed quote keeps its real date and can publish successfully.
An empty EIA API window is valid only after corroboration with its published
table; it emits no_observations with zero publishes and null observation bounds.
An EIA disagreement or malformed response fails instead. Empty NASA parameter
maps are invalid, rather than evidence of a source holiday. Receipt status is
not permission to release stale/empty inputs: the approved age, coverage and
required-feed gates remain the responsibility of items 13–16.

Credential-free tests use the real Functions Framework Flask HTTP handler and
Gen2 Pub/Sub CloudEvent conversion with packaged roots, real parsing/Avro and
stubbed external IO. They check HTTP status, absence of completion receipts and
credential canaries in response bodies, stdout/stderr and exception logs. Unit
tests also cover mismatched series, invalid later records before any publication,
out-of-window/noncanonical dates, nonfinite/boolean measurements and failed
publisher initialization. Network access remains blocked throughout these tests.

## Verified deployment: 28 October 2026

Implementation e654122 on codex/producer-failure-signals passed 258 tests on
Python 3.11, Ruff, 37 Python and five shell syntax checks, deterministic packaging
and pip check. The adapter tests emit 13 non-failing deprecation warnings from
Functions Framework's CloudEvents conversion; no application test was skipped.

The saved Terraform plan changed only three source pointers. All four active
functions remain python311; triggers/retry policies, identities, IAM and resource
limits were preserved. The actual serving archive members match the tested
repository inputs, including the pinned requirements. The consumer was unchanged.

| Feed | Serving revision | Source generation |
| --- | --- | --- |
| Cocoa | publish-price-00019-gik | 1793224804484938 |
| Oil | publish-eia-data-00004-hoq | 1793224806181398 |
| Weather | publish-weather-data-00013-quq | 1793224807023093 |

Controlled run producer_failures__e654122__20261028 passed all ten tasks between
23:04:57 and 23:06:03 Europe/Berlin (UTC+1), outside the normal service window.
Its BigQuery job was
airflow_cocoa_daily_run_bq_pipeline_2026_10_28T22_04_51_00_00_e72de539e630c9e04d4376c1bd521397.

- Cloud Logging recorded two ICCO, one EIA and one NASA_POWER completion receipts.
  Their acknowledged counts exactly matched two, ten and eight new staging rows.
  Actual observation ranges were cocoa 27 October, oil 14–27 October, and
  weather 21–28 October; acquisition timestamps were separate, aware instants.
- Each feed had valid provenance and zero curated reconciliation errors.
- All 13 quality checks returned zero violations. Joined data, features and
  historical predictions each contained 4,683 unique dates through 28 October.
- All 40 unique forecasts covered 29 October–7 December, and three BI source
  views had no null outputs. Rendered dashboard appearance was not tested.

Failure/empty-window injection was performed in credential-free real-adapter
fixtures, not by sending poison inputs to production. The deployed controlled
cycle exercised successful real requests, publication, storage and analytics.
The existing permissive Airflow completion rules and missing durable raw
acquisition archive remain open under items 13–15 and 10 respectively.

## Recovery and handoff

Six accepted-table snapshots and a model copy were preserved in
cocoa-prices-430315.producer_failure_recovery_20261028_e654122. The copied model
reproduced all 40 accepted forecasts within 1e-8. The three new source archives
and three rollback copies have explicit, re-read temporary holds and verified
checksums. Rollback archives and the completion manifest use the private prefix
gs://raw-historic-data/operations/2026-10-28/producer-failure-signals-e654122/.

For code rollback, pause and reconcile active work, reupload the exact held
before-archives to immutable source objects, deliberately update source generations,
review a source-only plan and verify actual serving bytes after applying. These
copies contain the accepted item 6 date/timestamp and request-budget behavior.
Table/model restoration is separate and must account for any later releases.

Final checks verified the VM TERMINATED, DAG idle/unpaused for 29 October at
19:00 Europe/Berlin, legacy Beam units disabled, and both Terraform roots with
no drift (93 managed resources). The private held completed_manifest.json records
all serving/rollback checksums, recovery jobs, task states, quality results and
the four real acquisition receipts. Its generation is 1793225551992366 and
SHA-256 is 76a748954057a22a433e8bb4f57ceeadbfdd81d516442d91c5120e0f1f92bce1.
No branch push, PR or hosted CI execution is claimed.
