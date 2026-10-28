# Bounded daily source requests

Checklist item 6 applies to the ICCO, EIA and NASA POWER producers. All use
the same packaged `request_policy.py`; the staging consumer's existing bounded
BigQuery request is unchanged. Source dates, timestamp provenance, Avro contracts,
weather coordinates/time standard and EIA API/published-table agreement remain
unchanged.

Each producer starts a 50-second cooperative budget within its configured
60-second function request limit. HTTP acquisition shares the first 40 seconds;
publication retains the remaining budget. Each HTTP attempt has explicit connect
and read inactivity limits, at most 5 and 20 seconds respectively, reduced when
the budget is short. EIA pagination and corroboration share one deadline.
Secret Manager access has a 10-second timeout with SDK retries disabled.

Only transport errors and HTTP 408, 429, 500, 502, 503 and 504 are retried, at
most three attempts per request, with 0.5/1-second backoff. A valid `Retry-After`
delay in seconds or HTTP-date takes precedence. If it cannot fit the remaining
budget, acquisition fails without retrying early. Other HTTP failures and invalid
source payloads fail immediately. Response bodies, query strings and SDK exception
messages are excluded from failure output.

Every publication checks the remaining invocation budget before submission and
waits at most 10 seconds, reduced to the remaining budget. A weather or oil
partial batch fails the invocation. A timed-out publish can still complete;
downstream date-key reconciliation remains necessary. This does not provide
exactly-once delivery or change Eventarc/Airflow retry policy.

Requests timeouts measure socket inactivity, not total DNS or body-transfer time.
The application checks elapsed time between operations and rejects late HTTP
responses; the 60-second platform request limit is a further response bound,
not proof that background SDK threads or all underlying work are terminated.

Credential-free fixtures cover persistent and recovered transport/status errors,
rate limits, shared deadlines, expired publish waits, partial weather publication,
and canaries in response bodies and exception messages. Packaging tests verify
all four producer inputs byte-for-byte and exclude private/generated files.

## Verified release: 28 October 2026

Implementation revision: 7363e2a, branch codex/bounded-source-requests.
229 credential-free tests pass on Python 3.11, including real Avro encoding,
HTTP-date/seconds rate limits, transport recovery/exhaustion and Secret Manager's
disabled SDK retry. Ruff, 36 Python and five shell syntax checks pass.

The saved Terraform plan changed only three producer source pointers. Runtime,
entry points, identities, trigger topics/retry policies, IAM and resource limits
were preserved. All four active functions use python311. All serving ZIP members
match their corresponding repository inputs byte-for-byte; the staging consumer
was unchanged. The three new source objects are protected by temporary holds.
Producer source ignore_changes was removed: all four functions now explicitly
pin verified source generations, avoiding the stale-seed rebuild demonstrated
during the earlier Python 3.11 migration.

| Feed | Serving revision | Pinned source generation |
| --- | --- | --- |
| Cocoa | publish-price-00018-gic | 1793223073388889 |
| Oil | publish-eia-data-00003-hac | 1793223074257112 |
| Weather | publish-weather-data-00012-sok | 1793223074876944 |

Controlled run bounded_sources__7363e2a__20261028 completed all ten tasks between
22:34:20 and 22:35:34 Europe/Berlin (UTC+1), outside the normal service window.
Its BigQuery job was
airflow_cocoa_daily_run_bq_pipeline_2026_10_28T21_34_13_00_00_adbd6a64d90e53755cbc669c943f4724.

- All 13 SQL quality checks returned zero violations.
- Two cocoa, ten oil and eight weather staging rows had valid acquisition,
  receipt and raw timestamp provenance. Each feed reconciled with its curated
  table with zero errors. These sample timings do not establish ongoing SLOs.
- Joined data, features and historical predictions each had 4,683 unique dates
  through 28 October. All 40 unique forecasts covered 29 October–7 December.
- Three BI view queries had no null outputs. Dashboard visuals were not tested.
- Airflow remained on Python 3.11.10; all legacy daily Beam units remained
  disabled/inactive. The DAG was left unpaused and idle for its next scheduled
  run at 19:00 Europe/Berlin on 29 October.

The earlier 19:00 scheduled run reported DAG success after wait_oil_rows skipped;
EIA logs classified its failure as an API request failure. This confirms the
known failure-semantics problem in item 13. A successful DAG state alone is
insufficient evidence of complete delivery. This release's controlled run met
the stricter all-ten-tasks and per-feed reconciliation gates; orchestration and
atomic-publication work remain open under their own checklist items.

## Recovery and handoff

Before promotion, six accepted tables were snapshotted and the model copied to
cocoa-prices-430315.source_request_recovery_20261028_7363e2a. All 40 predictions
from the copied model matched the accepted forecasts within 1e-8. No staging
history was rewritten.

Held, pre-change producer archives are under
gs://raw-historic-data/operations/2026-10-28/bounded-source-requests-7363e2a/.
For source rollback, first pause and reconcile active work, copy those exact
archives to new immutable source objects, explicitly update their Terraform
generations, verify a source-only plan, apply and compare serving bytes. Do not
restore the old seed pointers: they predate the date/timestamp corrections.
Table/model restoration is a separate operator action and requires reconciling
any releases after these snapshots.

Final checks verified the VM TERMINATED, DAG unpaused/idle, and both Terraform
roots with no drift (93 managed resources). All three source and three rollback
archives have explicit, re-read temporary holds; setting a hold before upload
alone did not persist it, so the final gate applied and verified metadata patches.

The private held completed_manifest.json at the evidence prefix records exact
source/rollback checksums and generations, recovery jobs, all task states and
validation results. Its generation is 1793223797855466 and SHA-256 is
7f5b8617b1740961b538a1eb517d7d3b26ec32e0a18f472108fb319b86201446.
No hosted CI run, branch push or PR is claimed by this local/deployed validation.
