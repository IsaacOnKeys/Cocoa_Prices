# EIA production cutover: approval package

Status: awaiting explicit production-cutover approval. Candidate checkpoint:
`63cb0f7cb1b41d629a7cb352771fd2ce57861fda` on `codex/eia-oil-migration`.

Automatic approval review rejected promoting the candidate into the existing
production oil topic while the legacy producer remained active. It identified
possible duplicate/conflicting ingestion and required approval of the exact
production sequence. The rejected command did not execute. Do not retry it
through another CLI, wrapper or deployment mechanism without resolving this gate.

## Verified candidate

- EIA function `publish_eia_data` is ACTIVE with entry point publish_eia_data.
- Input: `eia-oil-trigger`; current output: `eia-oil-validation`.
- The real deployed trigger, schema-bound topic, real streaming Beam consumer
  and temporary BigQuery sink produced six corroborated observations. The
  successful validation dataset was deleted afterward.
- Eventarc HTTP 403 was diagnosed and fixed with a service-scoped invocation
  grant for its actual trigger account; the repeated cloud test passed.
- The serving ZIP's three members match reviewed source/canonical schema bytes.
- EIA published-table history is archived under a new held object, with exact
  generation recovery and checksum verification. Existing warehouse history
  has not been replaced or truncated.
- 104 credential-free tests and Ruff passed. Python/shell syntax, XML diagrams,
  JSON sample, YAML manifest and the installed-container DAG graph were checked.
- The API key remains in Secret Manager; candidate/source-artifact scans found
  no persisted matches. No secret versions are managed through Terraform state.

## Preflight and rollback evidence

The actual installed Airflow DAG has SHA-256
`40265cf6a43faaa2378e1a17020d2ef59be27d781bf3bc81cd521473c7b9b1ff`.
The validated candidate DAG has SHA-256
`c65aa41e2eee9cd417cfecd966d77e27da5f068eb1a21371db4fd3855922a8fc`.
Its ten task IDs, dependency graph and schedule match the installed DAG. Only
the default oil trigger changes to eia-oil-trigger. The deployed override
Variable was absent, and there were no running cocoa_daily DAG runs.

The original pause state was unpaused. The temporary preflight pause was
reversed after the rejection, and the original installed DAG hash was verified
unchanged. The VM was started solely for preflight outside its normal window;
its original stopped state is restored before the scheduled 18:30 Berlin start.
Normal Cloud Scheduler and weekday DAG schedules are preserved.

VM backup directory: `/opt/airflow/releases/eia-63cb0f7/`. It contains the
original/candidate DAG bytes and ownership/mode receipt (UID 1001, GID 0, 0644).
Local private evidence is under `.build/eia-migration/`, including original
function source, non-secret configuration, invocation policy, pre-migration
Terraform state, isolated plans and validation receipts. The original serving
function source ZIP has SHA-256
`185a68a7b909572c22acfd4a0d75119af6e8bceba13e28fc438b78a8e8c80d6d`.

Legacy identifiers in this migration-only approval package describe existing
resources accurately. They are not names used by the new acquisition code.
Current production is `publish_fred_data`, invoked from `oil-trigger`; its API
credential is `FRED_OIL_API_KEY`. Preserve that credential and source artifact
for rollback rather than deleting them as a cosmetic rename.

## Proposed approved sequence

1. Confirm the actual current VM/DAG/trigger state and no active daily run;
   choose a controlled window that does not collide with the normal daily run.
2. Pause cocoa_daily and preserve the current configuration/ownership receipt.
3. Disable the legacy producer's Eventarc invocation first, using its backed-up
   service-level invocation policy and actual trigger identity. Verify the
   legacy route is quiescent before enabling EIA production publication.
4. Review a plan scoped to the already verified EIA function's output change
   from eia-oil-validation to oil-prices-topic. Its trigger stays eia-oil-trigger;
   existing consumer, topic/schema and staging interfaces stay compatible.
5. Apply that reviewed promotion, confirm ACTIVE/serving configuration, and
   install the already parsed candidate DAG while preserving file owner/mode.
   Reconcile any actual Airflow Variable override rather than assuming a default.
6. Run one controlled daily cycle. Require explicit task-level evidence for all
   three producer triggers and sensors, the BigQuery procedure and summary;
   a green DAG with skipped required work is insufficient acceptance evidence.
7. Verify EIA source/corroboration markers and expected dates/prices in the real
   staging output, curated alignment, finite/unique correctly dated forecast
   outputs, and consumer-facing data status. Record job/run/release identifiers
   and the remaining evaluation/freshness limitations separately.
8. On failure, restore the original DAG/pause/trigger policy, return EIA output
   to validation and verify the legacy route is the sole production producer.
   Source rollback does not erase acknowledged messages or warehouse writes;
   assess and reconcile those effects explicitly.
9. On success, restore the original unpaused state and normal VM scheduling.
   Retire the superseded function/trigger and reconcile Terraform state and
   protected-resource lifecycle handling without disabling unrelated guards.
   Preserve the legacy API key and code for the agreed rollback window.
10. Refresh inventory and all active provider references, document the final
    deployment/rollback receipt and mark the source migration complete. Retain
    original dated provenance through exact Git/archive references.

This approval covers the producer/routing cutover and retirement of its
superseded function after validation. Historical warehouse replacement,
WRITE_TRUNCATE recovery, model redesign, API-key deletion and later checklist
features are separate work. The 28 old-only dates remain an explicit new-source
history difference governed by the accepted source policy.

## Required owner decision

Approve the sequence above, including disabling the legacy producer before
promotion and retiring its superseded function after validation, while keeping
the old credential and source for rollback; or leave the verified candidate
isolated until a later cutover window. The production switch remains blocked
until that decision is explicit.
