# Daily staging consumer

stage_analytics_data is a bounded HTTP handler hosted as a private Gen2 Cloud
Run function. Authenticated Pub/Sub push delivers existing binary Avro messages;
source producers, trigger topics, units and staging table columns are preserved.
Beam remains available for backfills and local compatibility checks.

Package with `python scripts/deploy/package_functions.py ingestion`. The prepared
root contains main.py, requirements.txt and all three byte-identical canonical
schemas. Deploy that root/archive, not this source directory alone. Runtime is
Python 3.12; it has no Beam or BigQuery SDK dependency.

GCP_PROJECT and INGESTION_ROUTES define an explicit subscription-to-feed/table
allowlist. There is no destination derived from an untrusted message field.
Cloud Run IAM authenticates the push identity; public invocation must stay
disabled. The delivery identity has a service-scoped invoker grant, and Pub/Sub
can mint its tokens through a grant scoped to that identity. The separate
runtime identity receives only staging-dataset writer access, with no project
query-job or secret-access grant added by this component.

Requests are limited to a 2 MiB envelope and one canonical Avro record. Each
request makes one BigQuery insertAll call, with bounded transport/refresh waits;
row-level errors and HTTP failures return 503. Success returns 204 only after
accepted storage. The platform enforces a 60-second timeout, one concurrent
request per instance, at most three instances and no minimum instances.
Logs contain outcome, hashed delivery identity, duration and process peak memory;
they omit payloads, tokens, sink response bodies and arbitrary exception text.

insertId is a stable best-effort deduplication hint derived from subscription
and message ID. It is not an exactly-once guarantee. Ambiguous writes can be
redelivered; durable message identity/idempotency is checklist items 8-9.
Invalid records remain unacknowledged; durable quarantine and DLQ/replay remain
items 11-12. The cocoa producer now preserves the publisher observation date. Cross-feed
timestamp corrections remain item 5; recurring publication gates remain later
work.

The guarded integration tool is
[validate_ingestion.py](../../scripts/diagnostics/validate_ingestion.py). Its
prepare/exercise/verify/cleanup phases use unique test namespaces. Exercise
requires the Airflow VM stopped, rejects unauthenticated calls, tests every
schema and nullable measurement, denies then restores test-sink access, and
requires both retry and eventual acceptance for the original message. Verify
can resume read-only polling when IAM/log propagation delays observation.
Cleanup refuses production or unrelated datasets/subscriptions.

See the [execution decision](../../docs/operations/production-execution.md) and
[deployment/rollback procedure](../../docs/operations/ingestion-cutover.md).
