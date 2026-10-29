# Acquisition windows and successful coverage

Checklist item 9, branch `codex/acquisition-coverage`. Implementation and
credential-free tests are complete; managed acceptance and production promotion
remain pending. The last accepted production release is item 8.

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
