# Retry ownership and duplicate delivery

Checklist item 8 preserves the daily Cloud Run topology and Python 3.11. The
changes below are pending managed integration and deployment acceptance.

## Budgets and recovery destinations

| Boundary | Retry owner and budget | Exhaustion |
| --- | --- | --- |
| Source HTTP | Shared producer policy: at most three transient attempts, remaining acquisition budget, Retry-After respected | Invocation fails without a completion receipt |
| Airflow acquisition trigger | Three attempts, three-minute execution limit each; 30-second exponential delay capped at two minutes; enqueue RPC timeout 30 seconds | Failed task; missing endpoint/404 fails permanently |
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

Managed validation, serving-byte checks and the complete daily cycle are pending.

The isolated managed probe 20261028_2b590e55 has now verified a policy update
without transport replacement, transient recovery after two attempts, poison
forwarding after five observed attempts, and a corrected replay retaining the
same acquisition identity with a new transport event ID. Source bytes matched
the uploaded probe archive; all temporary function, transport, topics, subscriptions
and counter objects were removed. These observed attempt counts are sample results.

The local Python 3.11 suite passes 288 tests; Ruff, 41 Python and five shell syntax
checks pass. Terraform validation passes, and a reviewed transport-only plan has
22 changes: 17 new recovery/IAM resources, two existing transport imports/updates
and three canonical subscription updates. It contains no deletions, function or
project-IAM changes, and preserves existing push routing, retention and identity.
Production application and final integration gates remain pending.
