# Acquisition windows and successful coverage

Checklist item 9, branch `codex/acquisition-coverage`. The acquisition component was deployed and validated on 2026-10-29. The
engineering cycle exposed a NASA freshness breach: its October 29 serving
candidate was preserved and withheld, and the verified October 28 outputs/model
were restored. Scheduling is held until required-feed/freshness enforcement is
installed. This is component acceptance, not production-service readiness.

## Window policy

Production supplies both `COVERAGE_RECEIPT_BUCKET=raw-historic-data` and
`COVERAGE_STATE_BUCKET=cocoa-prices-temp-for-bq`. Each source requests from its
last publish-acknowledged window end minus 30 calendar days through today's UTC
acquisition date. Initial collection requests the latest 31 calendar dates.
This preserves at least 30 days of correction overlap and expands after missed
runs. The entire request must fit 90 calendar dates and the existing 50-second
cooperative budget; longer recovery fails explicitly for controlled backfill.
This is a bounded engineering policy, not a claim that upstream revisions never
occur earlier. Older revisions/withdrawals and recovery beyond the bound require
the controlled backfill/revision policies in items 17 and 29.

EIA continues to publish every validated API observation and requires exact
non-missing date/value agreement with its published table for the entire requested
window. Its existing pagination, source/units validation and secret handling are
preserved. A genuinely empty, corroborated window is explicit; malformed or
unexplained incomplete responses fail.

Cocoa uses ICCO's ordinary public statistics table, offered 100-row pages, sorted
newest first. It reads through the lower boundary, checks pagination totals,
ordering, duplicate dates, units/columns and finite positive prices, and publishes
every dated observation in the window. Entirely empty undated publisher rows are
not observations. Failure to reach the boundary fails collection. The latest
homepage quote must agree wherever it overlaps and is included if exposed there
first. No unavailable source history or market-closure reason is invented.

NASA retains the configured point, parameters and LST daily dates. Both parameter
maps must include every requested calendar date. Explicit null/sentinel values
remain recorded missing measurements; an absent date/key is an incomplete
collection failure. A successful response does not make a missing value current.

Unconfigured local/legacy invocations retain the old collection behavior; partial
bucket configuration fails. Acceptance must verify both settings on all three
serving functions. The consumer and canonical Avro schemas are unchanged. Cocoa
and NASA now also accept an output-topic setting for guarded isolated validation;
production defaults retain their existing topics.

## Durable coverage and revisions

The shared acquisition policy stores immutable create-only JSON receipts under
`acquisition-coverage/v1/<source>/receipts/<acquisition_id>/<content_sha256>.json`
in the receipt bucket. Each retains the requested window, actual published
inventory and observation revision hashes, explicit missing-measurement names,
and calendar dates not exposed in the successful response. The latter are source
absence evidence, not assumed holidays. Complete raw source retention is item 10.

A generation-guarded compact `state.json` in the state bucket joins only touching
or overlapping successful windows. Out-of-order completion cannot decrease its
frontier or replace a more recent completion pointer. At most three write races
are retried within the remaining budget. Immutable receipt persistence precedes
state advance; every source publication must already be acknowledged. Partial
publication, failed corroboration or a failed receipt write cannot advance it.
A retained initialization marker detects a lost projection and requires a rebuild
from receipts instead of silently restarting at a recent baseline. No newly
initialized state claims coverage before its first actual acquisition window.

This stage is named `publish_acknowledged`: it does not certify BigQuery receipt,
source age, required-feed completion or release acceptance. Those remain items
13-15 and 19-20. Existing staging stores distinct value-revision fingerprints;
curated updates for the tested older-correction ordering use the later completed
acquisition. General ties, invalid-null precedence and source withdrawals remain
item 17. No previously unknown acquisition time is reconstructed from new history.

The producer identity already has receipt-bucket create/view and state-bucket
object-admin access; no new project grant or production bucket is introduced.
Secret values never enter the receipts. SDK operations disable implicit retries,
use generation preconditions and at most five seconds reduced to the remaining
invocation budget. State lives in the existing temporary staging bucket, which
currently has no deletion lifecycle. Loss is detected; operational retention,
backup and restore verification remain items 10 and 28.

## Validation

The Python 3.11 suite passes 334 tests, including the publisher adapter regression and seven validation-namespace
guard cases; Ruff, 49 Python and five shell syntax checks pass. Tests exercise multi-day expansion, older
corrections, explicit source absence versus missing measurements, duplicate and
out-of-order completions, generation races/exhaustion, lost projections, complete
Cocoa parser windows, partial batches and real HTTP/Gen2 failure responses when
coverage persistence fails. Ruff and Terraform validation pass.

The [managed validator](../../scripts/diagnostics/validate_coverage.py) creates one
private, max-one-instance Python 3.11 probe containing byte-identical producer
modules, shared policy and Avro contracts. Its two disposable buckets, trigger,
three output topics/subscriptions and scoped bucket grants are separate from
production. A simulated five-day-old frontier drives live-source collection;
controlled source/batch failures and a correction more than two weeks old verify
coverage and revision behavior with real GCS/Pub/Sub. It reconciles every declared
revision with decoded deliveries, then guarded cleanup removes all resources.

Primary references: [ICCO daily price table](https://www.icco.org/statistics/),
[NASA daily dates/time standards](https://power.larc.nasa.gov/docs/services/api/temporal/daily/),
and [Cloud Storage generation preconditions](https://docs.cloud.google.com/storage/docs/request-preconditions).


## Managed and deployed evidence: 29 October 2026

Implementation `e28a43d`; final validator fixes `ff00b33`. All 334 credential-free
Python 3.11 tests, Ruff, 49 Python and five shell syntax checks pass. Terraform
validation passes. Managed probe `20261029_a2f48a3a` packaged the byte-identical
three producer modules, shared policy and schemas, and verified every member
against the submitted source. It used a private Python 3.11 function (one instance,
512 MiB/one CPU) and disposable buckets/transport/output routes. This test resource
size is distinct from the preserved production limits, which were subsequently
exercised by the full daily cycle.

All fifteen live/fixture scenarios passed. The simulated five-day-old frontier
expanded live acquisition to September 24-October 29: 24 Cocoa, 24 EIA and 36 NASA
observations. Per-feed source failure and a controlled failure after the first
acknowledged publish left coverage unchanged. October 9 corrections retained
one acquisition identity with a distinct value revision. All declared revisions
arrived and decoded correctly: Cocoa 48 revisions/71 deliveries, EIA 48/71 and
NASA 68/99, including exactly one extra controlled partial revision per feed.
All probe resources, source objects and temporary scoped bucket grants were
removed. Two earlier probe attempts exposed fixture numeric-type and static-SDK
binding mistakes; both were corrected before acceptance, and their resources
were also removed. A regression covers the actual publisher wrapper binding;
received evidence is persisted before acknowledgement so validation can resume.

A saved, reviewed plan changed exactly three producer source generations and
the two coverage settings per producer. Runtime, entry points, existing output
routes, identity, trigger retry policy and resource limits were preserved; no
production IAM or bucket was added. The consumer and installed DAG were unchanged.
All five prior delivery policies/destinations/forwarding grants and authenticated
source routing were read back successfully after promotion.

| Feed | Accepted serving revision | Source ZIP SHA-256 |
| --- | --- | --- |
| Cocoa | publish-price-00021-yik | 1d52214847342e92ef807be7c63967dcb1c83e8bf64609a4d25b4f6c88f2b7c1 |
| EIA | publish-eia-data-00006-div | 0bbb0536f1aa56e73df18d90d881e67f0c084d89b9ddb09cd5c69bb2f8d9970c |
| NASA | publish-weather-data-00015-bag | bd60ddf599eefee524e15ad8b48d9213aa726b449b6ac062242be7c0cf200f54 |

All four serving functions use python311 and match repository bytes. The
unchanged consumer remains `stage-analytics-data-00010-jiv`. The installed
Python 3.11.10 / Airflow 2.10.2 DAG remains SHA-256
`fe1f8863e451f8fae09db5c218daebca0debe00c4833d6b1dc7b2da71633e0ba`,
with verified owner/mode and ten parsed tasks. Legacy daily Beam units remain
disabled/inactive.

Controlled cycle `acquisition_coverage__e28a43d__20261029` ran from 09:17:21 to
09:18:38 UTC (10:17-10:18 Berlin), outside the contracted acquisition window.
All ten tasks succeeded. The initial production request was September 29-
October 29. Exactly 21 Cocoa, 21 EIA and 31 NASA observations reconciled among
held-source code, Cloud Logging completion receipts, durable inventories and
BigQuery rows for the exact acquisition/attempt. Delivery/revision hashes and
all timestamp/source-to-curated comparisons passed. Thirteen existing SQL checks,
forty unique finite positive forecasts and three BI source queries passed for
the engineering candidate. This does not make its source data fresh.

### Actual freshness breach and withheld candidate

Cocoa/EIA exposed quotes through October 27 (two days old, within their seven/
fourteen-day limits). NASA supplied every requested date, but both measurements
were missing October 22-29. Its latest valid measurements were October 21, eight
days old on October 29, beyond the agreed seven-day maximum. The publisher's
[normal meteorological latency guidance](https://power.larc.nasa.gov/docs/faqs/data/)
remains two to three days; that is not a guarantee or evidence explaining this
particular delay. No cause or market-closure explanation was invented, and the
accepted age limit was not relaxed.

The existing filled calendar and thirteen SQL checks did not detect this source
freshness breach. The candidate is explicitly not a service-compliant release.
Six candidate table snapshots and a model copy preserve its engineering results.
With no active DAG run, the verified previous October 28 model was restored;
all six serving tables were restored together in a transaction with candidate
preimage assertions. Each restored table exactly matches its recovery snapshot,
and all forty model predictions match the restored forecasts within 1e-8.
New staging facts, raw coverage receipts and progress were retained unchanged.

Serving joined/features/retrospective outputs now end October 28 (4,683 rows);
the forty prior forecasts run October 29-December 7. All three BI source queries
match that prior date; charts were not visually checked. The daily DAG is paused
and idle, and the VM is restored stopped outside its window. The normal next
schedule is October 29 at 18:00 UTC, but the deliberate pause prevents publication.
Required-feed/freshness enforcement (item 13) is prioritized before resumption;
items 10-12 retain their numbering and remain open. Generic publication/forecast
failure atomicity remains item 20; this controlled restoration does not complete it.

### Recovery records

Dataset `cocoa-prices-430315.acquisition_coverage_recovery_20261029_e28a43d`
retains six before-cycle snapshots/model plus the six withheld `candidate_*`
snapshots and `model_candidate`. Before-copy predictions and candidate-model
predictions each matched their forty forecasts within 1e-8. Snapshot job:
`31b9c1b4-bff2-43c7-a4d9-9b9ae8306a59`; model copy:
`d97e199a-5ae2-456b-ae1f-a508a5adca13`; initial model comparison:
`5fb909e5-3669-4a9d-9d79-4b0274ae3c70`. The restored prior source dataset is
`cocoa-prices-430315.retry_delivery_recovery_20261029_94fe21e`.
Candidate snapshot/model jobs are `0fd454b5-ba4d-414c-a20e-763db0eaeaa4` and
`b67afdcf-2368-47cc-ad71-dd42dede8971`; atomic restoration job:
`0aa2587a-1304-420c-9d11-a084408608e8`; restored prediction comparison:
`24b27f8a-5f82-44ce-baa8-5c175d4238fa`.

The private evidence prefix
`gs://raw-historic-data/operations/2026-10-29/acquisition-coverage-e28a43d/`
retains three checksum-verified held before-source archives and a held completed
manifest. The three new source generations are held separately in the GCF bucket.
The manifest records precise code/generations, isolated validation/removal,
coverage and completion receipts, candidate checks, the failed freshness
assessment, withheld artifacts, serving restoration, final paused/stopped state
and both no-drift Terraform plans (112 managed resources). No current-source,
SLO, chronological forecast accuracy, hosted CI or rendered BI acceptance is
claimed.

Completed manifest generation: `1793267124662426`; SHA-256:
`93640e2d626cdfcd733ca7b3e4fdc7c9e99ec63b58e34f93b10a36d277c24b9a`.
