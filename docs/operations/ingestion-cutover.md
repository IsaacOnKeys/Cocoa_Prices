# Bounded daily consumer cutover

Owner decision: Cloud Run daily consumers, retaining Beam for backfills.
Branch: codex/production-execution. Status: isolated acceptance passed;
controlled production handoff prepared. Existing producer deployments and
acquisition trigger topics are unchanged.

## Verified candidate

The private stage_analytics_data function is ACTIVE in europe-west3 with
Python 3.12, one CPU, 512M memory, concurrency one, maximum three instances,
minimum zero and a 60-second timeout. Runtime and push identities are distinct:
analytics-staging and analytics-push. There is no new project-wide IAM grant.
The source archive has SHA-256
`41acd6ddebaaa396974df7f0ee3435081336713ee65f42581a45adb61dc64a95`.
The actual serving archive's five files match the reviewed handler/requirements
and canonical cocoa/oil/weather schemas.

The isolated route delivered two records per feed, including nullable
measurements, while airflow-vm was TERMINATED. Unauthenticated HTTP invocation
was denied. The first oil delivery returned retryable failures while its
runtime lacked sink access; zero rows existed at that checkpoint. Granting
test-dataset access allowed the original delivery and all six fixtures to be
accepted without a service restart. Observed process peak memory was 54,652
KiB, with the first failed application request taking 1,170 ms. These are
measurements of this small test, not latency or capacity guarantees.

The initial verification reached the correct rows before its acceptance log
became visible, causing a diagnostic assertion failure. A read-only resumed
verification confirmed both retry and acceptance for the same delivery. The
tool now polls for delayed logs; a regression test covers that race.
Credential-free checks: all 133 tests passed, including namespace and delayed-log
regressions. Ruff, package membership, 28 Python/five shell syntax checks and
Terraform formatting/validation passed.

## Production sequence

1. Preserve current Terraform state, production subscription configuration,
   installed DAG and Beam unit/source state. Start the VM outside its normal
   window only for controlled verification.
2. Parse the candidate DAG in the actual Airflow container, confirm no running
   or queued daily run and commit a DAG pause. Record the original pause state.
3. Stop all three old Beam services and verify inactive state before enabling
   push delivery. Keep their enablement/files available until acceptance.
4. Apply the reviewed four-update plan: route allowlist changes to the three
   production subscriptions; those existing subscriptions become authenticated
   push with a 60-second acknowledgement deadline. Retention, topic identities
   and backlog remain in place; nothing is deleted or recreated. Staging access
   is granted separately beforehand to allow permission propagation.
5. Install the comment-only DAG update preserving owner/mode, restore its pause
   state, and trigger one controlled daily cycle. Require all ten tasks and the
   real source/curated/forecast/BI output checks to pass.
6. After acceptance, disable all three legacy Beam startup units, preserving
   files for rollback. Verify orchestration no longer depends on them. Recover
   receipts, restore the VM's original state and leave normal schedules intact.
7. Remove temporary validation subscriptions/topics/dataset with the guarded
   cleanup tool, refresh both Terraform plans/inventory and document results.

## Rollback and support boundaries

Pause the DAG and confirm no active run. Review/apply the configuration with
daily_ingestion_push_enabled=false and, when available, the preserved isolated
route file. This removes push delivery in place while retaining subscriptions
and backlog. Account for in-flight requests, allowing their bounded timeout
before enabling/starting the three saved Beam units. Restore the previous DAG
with its recorded owner/mode and pause state. Verify the old route is the only
consumer and run the complete output checks. Restore Terraform ownership and
VM scheduling consistently; do not delete/recreate subscriptions to switch modes.

Source rollback does not reverse acknowledged writes. Reconcile duplicates or
ambiguous effects through measured repair, not historical WRITE_TRUNCATE.
The old DirectRunner path is an emergency rollback mechanism; it does not meet
the newly selected production execution standard. The new managed consumer
does not remove Airflow/PostgreSQL's VM dependency. External missed-run alerts,
runtime restore, durable quarantine, idempotency and atomic publication remain
their numbered checklist items.

Private evidence: `.build/ingestion/` contains source verification, validation
receipt, before-state snapshots, reviewed plans and apply logs. VM backups and
receipts use `/opt/airflow/releases/ingestion-41acd6d/`.
