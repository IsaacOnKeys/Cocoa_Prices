# Daily staging consumer

stage_analytics_data is a bounded HTTP handler hosted as a private Gen2 Cloud
Run function. Authenticated Pub/Sub push delivers existing binary Avro messages;
producer topics, units and original staging columns are preserved. Nullable
provenance columns retain acquisition and receipt separately.
Beam remains available for backfills and local compatibility checks.

A supplied observation revision hash must match the decoded source/date/business
values. A mismatch is durably quarantined before staging. Missing legacy
identities remain NULL and cannot satisfy a current exact-receipt gate. See
[revision-policy acceptance](../../docs/operations/deterministic-source-revisions.md).

Package with `python scripts/deploy/package_functions.py ingestion`. The prepared
root contains main.py, requirements.txt and all three byte-identical canonical
schemas. Deploy that root/archive, not this source directory alone. Runtime is
Python 3.11; it has no Beam or BigQuery SDK dependency.

GCP_PROJECT and INGESTION_ROUTES define an explicit subscription-to-feed/table
allowlist. There is no destination derived from an untrusted message field.
Cloud Run IAM authenticates the push identity; public invocation must stay
disabled. The delivery identity has a service-scoped invoker grant, and Pub/Sub
can mint its tokens through a grant scoped to that identity. The separate
runtime identity receives staging-dataset writer access and create/read access
conditional on the quarantine object prefix, with no project query-job or
secret-access grant added by this component.

Requests are limited to a 16 MiB envelope and one canonical Avro record. Healthy
requests make one BigQuery insertAll call. Permanent invalid input/row rejection
receives 204 only after its complete envelope is stored and read-back verified
in immutable quarantine. Transient or ambiguous sink failures retain diagnostic
evidence and return 503; an unverified quarantine also returns 503. The platform
enforces a 60-second timeout, one concurrent
request per instance, at most three instances and no minimum instances.
Logs contain outcome, hashed delivery identity, duration and process peak memory;
they omit payloads, tokens, sink response bodies and arbitrary exception text.

insertId is a stable best-effort deduplication hint derived from subscription
and message ID. It is not an exactly-once guarantee. Ambiguous writes can be
redelivered; durable message identity/idempotency is checklist items 8-9.
See [quarantine and native validation](../../docs/operations/invalid-record-quarantine.md).
Quarantined records do not establish required-feed completion; incomplete input
withholds publication. DLQ operations/replay remain item 12. The cocoa producer
preserves the publisher observation date. All feeds retain
UTC producer acquisition, raw timestamp text and consumer receipt separately;
unknown/invalid acquisition stays NULL with a status flag. See the
[timestamp contract](../../docs/operations/timestamp-provenance.md). Required-feed
and recurring quality gates use the accepted receipt's exact revision inventory.

The guarded integration tool is
[validate_ingestion.py](../../scripts/diagnostics/validate_ingestion.py). Its
prepare/exercise/verify/cleanup phases use unique test namespaces. Exercise
requires the Airflow VM stopped, rejects unauthenticated calls, tests every
schema and nullable measurement, denies then restores test-sink access, and
requires both retry and eventual acceptance for the original message. Verify
can resume read-only polling when IAM/log propagation delays observation.
The timestamps phase additionally verifies offset/missing/invalid/legacy-naive/
future cases and replay through actual storage. Temporary validation routes can
coexist with the exact unchanged production routes. Cleanup refuses production
or unrelated datasets/subscriptions.

See the [execution decision](../../docs/operations/production-execution.md) and
[deployment/rollback procedure](../../docs/operations/ingestion-cutover.md).
