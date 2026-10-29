# Retry ownership and duplicate delivery

Checklist item 8 preserves the daily Cloud Run topology and Python 3.11. The
changes below were deployed and accepted on 2026-10-29 (Europe/Berlin, UTC+1).

## Budgets and recovery destinations

| Boundary | Retry owner and budget | Exhaustion |
| --- | --- | --- |
| Source HTTP | Shared producer policy: at most three transient attempts, remaining acquisition budget, Retry-After respected | Invocation fails without a completion receipt |
| Airflow acquisition trigger | Three attempts, three-minute execution limit each; 30-second exponential delay capped at two minutes; publish acknowledgement wait 30 seconds | Failed task; missing endpoint/404 fails permanently |
| EIA/weather Eventarc transport | Retry enabled only after DLQ/IAM readiness; 10–60-second backoff, five delivery attempts requested, existing one-day retention | Separate source-trigger dead-letter topics/subscriptions |
| Three canonical staging subscriptions | 10–60-second backoff, five delivery attempts requested, existing seven-day retention | Existing per-feed dead-letter topics with durable pull subscriptions |
| Dead-letter destinations | Seven-day retention; no automatic republish | Operator-controlled inspection and replay after correction |

Five attempts is an approximate Pub/Sub forwarding threshold, not an exact
delivery guarantee. Correct publisher/subscriber service-agent IAM is required
for counting and forwarding. Retention bounds the retry window; expired messages
are not a recovery mechanism. DLQ alerting and retention/recovery operations
remain required under items 12, 26 and 28.

Eventarc retains authenticated endpoint/OIDC and management-label ownership.
Terraform imports its two current transport subscriptions and manages their
retry/dead-letter settings without rewriting push routing. Before any trigger
replacement, reconcile the actual transport names and forwarding permissions;
do not recreate a stale subscription with missing push configuration. DLQ pull
subscriptions exist before forwarding can be enabled. New service-agent grants
are resource-scoped publisher/subscriber permissions, rather than project grants.

Primary references: [Eventarc retry/subscription ownership](https://docs.cloud.google.com/eventarc/docs/retry-events),
[Cloud Functions retry policy](https://docs.cloud.google.com/functions/docs/reference/rest/v2/projects.locations.functions#RetryPolicy),
and [Pub/Sub forwarding and best-effort attempt counts](https://docs.cloud.google.com/pubsub/docs/dead-letter-topics).

## Identity and side effects

Airflow passes a logical acquisition key derived from DAG/run/feed through the
Cocoa HTTP header or trigger-message attributes. Producers hash that key with
source/scope; legacy events fall back to their source/event ID, and standalone
invocations without either receive a new identity. Transport retries and retried
enqueue calls preserve the logical acquisition identity. Raw caller keys are not
logged. Cocoa HEAD requests acknowledge readiness without acquiring or publishing.

Canonical messages carry acquisition_id and observation_id attributes. The latter
hashes the source, actual date and business measurements, excluding acquisition
and receipt timestamps; a changed measurement produces a new revision identity.
These are correlation/revision identifiers, not a distributed lock preventing
upstream refetches. The canonical Avro layouts and Beam backfill inputs remain
unchanged.

The consumer stores acquisition_id, observation_id, pubsub_message_id and
delivery_id in additive nullable staging fields. Delivery identity remains the
hash of subscription/message ID. Legacy messages retain null logical identities;
historical rows are not rewritten or assigned fabricated IDs. Supplied logical
identifiers must be 64 hexadecimal characters. Invalid attributes are not ACKed.
EIA records its acquisition instant after API/table corroboration completes, so
curation recency reflects completed collection rather than the earlier request start.

Staging is an append record of accepted deliveries. BigQuery insertId is a
best-effort hint, not exactly-once storage. The existing curated MERGEs choose
one source observation per date, so physical duplicate deliveries do not create
duplicate curated date facts. Corrected measurements remain distinct revisions;
coverage/correction and deterministic release selection still require items 9,
13–16 and 20.

Source-trigger failures now propagate through retryable Airflow exceptions;
missing configuration/404 does not silently skip. Required-feed completion,
receipt-to-run binding and the quiet-period/publication gates remain open under
items 13–15.

Native validation found that the installed Google provider's PubSubHook.publish
does not accept a timeout argument. The trigger uses the hook's authenticated
PublisherClient directly and bounds its publish-future wait instead. Task
execution timeouts bound the complete invocation; a timed-out future can still
complete in the background, so logical identity/deduplication remains necessary.

## Validation gates

Credential-free tests exercise real adapter identity propagation, read-only HEAD,
finite request/trigger budgets, invalid identity rejection and stable delivery
keys with new transport IDs. Terraform validation must pass before cloud changes.

The isolated [delivery validator](../../scripts/diagnostics/validate_delivery.py)
creates a private max-one-instance Gen2 probe with its own trigger/DLQ and GCS
counter prefix. It tests a no-retry-to-retry policy update, transient recovery,
poison-message forwarding and one corrected replay retaining acquisition identity.
It uses the actual shared identity helper. Namespace/label/source guards prevent
cleanup from targeting production, and all temporary resources must be removed.
Production receives no deliberately poisoned events.

Managed acceptance passed on Python 3.11. All 294 credential-free tests, Ruff,
42 Python and five shell syntax checks passed. The installed Airflow 2.10.2 /
Google provider 10.22.0 / Python 3.11.10 parsed all ten tasks, injected the run
context and serialized the new trigger budgets. Serving runtime changes are in
5acb5dd and 94fe21e; the isolated identity/MERGE driver is in 44e2e87.

The private managed probe `20261028_2b590e55` verified a no-retry-to-retry policy
update without transport replacement, transient recovery on attempt two,
permanent-failure forwarding after five observed attempts, and corrected replay
retaining acquisition identity with a new transport event ID. Source bytes
matched. Its function, native transport, topics, subscriptions, source archive
and counter objects were removed. Attempt counts are observations, not guarantees.

Isolated consumer validation `20261028_cba277af` ran with the VM stopped and
unauthenticated access denied. A deliberately denied sink recovered after its
scoped dataset grant. All feeds passed valid/null and timestamp cases including
missing, invalid, naive, future and replayed acquisition times. Three additional
messages per feed used one acquisition and two observation revisions, with the
newer correction delivered before older duplicates. Delivery hashes and logical
identities reconciled. Running the actual production MERGE statements twice in
isolated tables retained one fact per date and the corrected values: cocoa
6501.25, oil 73.5, precipitation 4.5 and soil moisture 0.42. This proves the tested
ordering, not general timestamp-tie precedence (item 17). No test facts entered
production. All temporary topics, subscriptions, tables/dataset and routes were
removed; the consumer again has exactly its three production routes.

The transport plan applied 17 new resources, imported/updated two existing source
transports and updated three canonical subscriptions. All five retry/dead-letter
policies, ten resource-scoped forwarding grants, destination subscriptions and
original authenticated push routes/retention were read back. Only then were the
EIA/weather function retry policies enabled. Native transport names survived the
function updates. Both Terraform roots now have no drift: 111 main resources plus
one bootstrap bucket, recorded in the [inventory](../infrastructure/gcp-resource-inventory.json).

Migration job `8617a5e3-e1d7-4739-bbf1-b2047aad7ce0` added four nullable STRING
identity fields to each staging table, preserving prior fields and historical
rows. The installed DAG SHA-256 is
`fe1f8863e451f8fae09db5c218daebca0debe00c4833d6b1dc7b2da71633e0ba`.

| Function | Accepted revision | Source ZIP SHA-256 |
| --- | --- | --- |
| publish_price | publish-price-00020-cav | d8da9782168b43ac011fa049e3b2c596e2095541c2f3447aa5e2261317987a12 |
| publish_eia_data | publish-eia-data-00005-xay | 0afc4119d77149cae2b8b897e216a41b15963a0fed19e6c2f230ca57bc4d84ef |
| publish_weather_data | publish-weather-data-00014-cas | 58845cfaa53286e018d8bdee6f9fc6d8610891dbe34ff4aa75b45b1392580582 |
| stage_analytics_data | stage-analytics-data-00010-jiv | 0220647226347cefecb24367fd1ed104c250f178661c0814fd3b7049f0e6f040 |

Every serving ZIP member matched repository source; all four runtimes are
python311. Consumer revisions 00008 and 00009 had the same bytes; the latter
attached isolated routes temporarily, removed again in 00010.

Controlled run `retry_delivery__94fe21e__20261029` passed all ten tasks from
00:16:27 to 00:17:31 UTC. Exactly one logical acquisition per feed reconciled
with completion receipts and accepted rows: one cocoa observation dated
October 27, nine EIA observations October 15-27 and eight NASA observations
October 22-29. Observation identities were recomputed from business values;
delivery identities matched subscription/message IDs. All timestamp and
source-to-curated checks passed. Thirteen quality checks passed; the forecast
contains 40 unique valid values for October 30-December 8. The joined/features
and retrospective scores each contain 4,684 daily rows through October 29;
three BI source queries passed. Charts were not visually checked. This remains
one controlled cycle, not measured scheduled reliability or achieved SLOs.

The VM was idle with the DAG unpaused, next schedule October 29 at 18:00 UTC;
legacy Beam units were disabled/inactive. It was restored stopped outside its
operating window after acceptance.

## Recovery evidence

Recovery dataset `cocoa-prices-430315.retry_delivery_recovery_20261029_94fe21e`
retains six before-release snapshots and a copied model whose 40 predictions
matched the accepted model within 1e-8. Snapshot job:
`63ad4485-a56b-4d90-9a00-e380fdaa8e66`; model copy:
`59b771b8-8ee9-454b-9e65-a5f30eac84dd`; prediction comparison:
`a4f0fb7b-44fe-4a45-bd45-cfaa1df3b7d4`.

The private evidence prefix
`gs://raw-historic-data/operations/2026-10-29/retry-delivery-94fe21e/`
contains four checksum-verified held before-source archives and a held
`completed_manifest.json`. Four new source ZIP generations are also held in the
GCF source bucket. The manifest records exact generations/checksums, schema and
recovery jobs, isolated test/removal receipts, run/tasks, source receipts and
identities, data checks, serving byte verification and final idle/stopped state.
The previous installed DAG is root-only under
`/opt/airflow/releases/retry-delivery-20261029/cocoa_daily.before.py`.

Rollback requires pausing/confirming no active run, restoring the four recorded
source pins and prior DAG, native validation and an operator-reviewed restoration
of the six table snapshots/model if data rollback is needed. Do not drop the
additive identity fields or erase accepted staging rows. Keep destination
subscriptions/IAM until affected deliveries are drained; disable source retries
before removing forwarding. Recheck actual transport names, push authentication,
source bytes and a complete controlled cycle before resuming normal operation.
The private prepared restoration SQL is retained with the local deployment
record; recovery snapshots and source objects are not Terraform-managed data.

Window recovery, durable raw evidence, operational DLQ replay/alerting,
required-feed/quiet-period gates, deterministic revision ties and consistent
release publication remain their separate checklist items. No hosted CI,
exactly-once physical staging, forecasting accuracy or rendered BI acceptance
is claimed.

Completed manifest generation: `1793233520745361`; SHA-256:
`3e9bcc00dcb1fee09948bb2adff257fb7c3f24d232e0c1b43d05b6b119624451`.
