# EIA production cutover: deployment and rollback receipt

Completed 2026-10-27 on `codex/eia-oil-migration`. The owner explicitly selected
option 1: disable the previous producer before promotion, verify a controlled
daily cycle, then retire its function/trigger while retaining its credential
and exact source for rollback. Implementation checkpoint: `63cb0f7`.

## Deployment sequence and acceptance

The earlier automatic approval rejection required this explicit sequencing
decision; its rejected command did not execute. After the owner approved:

1. Confirmed no running daily DAG and preserved its source/configuration.
2. Removed only the previous Eventarc identity's service-scoped invocation
   grant. Verified that identity had no legacy-service grant or public-access
   substitute, while retaining its new-service grant.
3. Reviewed and applied a saved Terraform plan containing exactly one update:
   EIA's output environment changed from eia-oil-validation to oil-prices-topic.
   The new trigger remained eia-oil-trigger. The function became ACTIVE at
   20:30:46 UTC.
4. Committed the DAG pause, installed the byte-verified candidate, parsed its
   ten-task graph in the actual container, and restored its original unpaused
   state. OIL_TRIGGER_TOPIC had no deployed Variable override. UID 1001,
   GID 0 and mode 0644 were preserved.
5. Triggered `eia_cutover__63cb0f7__20261027`, 20:32:45-20:34:01 UTC.
   All ten tasks succeeded: baseline, three producer triggers, three fresh-row
   sensors, settle task, BigQuery procedure and summary. None was skipped.
6. Verified source/curated alignment, forecasts, features and BI serving views
   as described below, before retiring any legacy function.
7. Deleted the superseded function and its generated Cloud Run/Eventarc
   resources. Verified its standalone trigger topic had zero subscriptions,
   then deleted that topic. Preserved the old credential container/source.
8. Detached only the retained credential container from active Terraform
   ownership using a backed-up state. A reviewed retirement plan removed
   exactly its unused service-scoped Airflow accessor grant. Refresh removed
   state for the externally retired resources. Shared prevent_destroy guards
   were unchanged. Both Terraform roots subsequently returned no-change plans.
9. Verified Airflow database/scheduler/triggerer health, all three consumers
   active with zero restarts, installed/rollback DAG hashes and permissions.
   Recovered the receipt locally and restored the VM's original stopped state
   outside its normal operating window. Existing schedules remain unchanged.

The first preflight pause was reported inside a transaction that had not
committed. A read-back before installation caught the unpaused state; the
installation precheck was corrected to commit explicitly. No DAG was running
and this occurred outside the scheduled run window. The old invocation stayed
disabled throughout the promotion. The corrected installation passed.

## Verified production results

Six EIA rows, dated October 13-16 and 19-20, reached the existing production
staging table. Every row identifies EIA/RBRTE, has published_verified=true,
and matches both the accepted published-table archive and curated Brent price.
There were six distinct dates, with no duplicate delivery in this test.
Latest oil ingestion: 20:33:18 UTC; latest observation: October 20. Receipt
freshness does not turn that observation into an October 27 quote.

All thirteen read-only feature/calendar/forecast/evaluation checks returned
zero violations. Joined data, features and historical predictions contained
4,682 daily rows through October 27. Forecasts contained exactly forty unique,
finite, positive values dated October 28-December 6. The KPI view returned one
complete row; both indexed-series views returned 4,682 rows with no null
outputs. All three freshness-view ingestion times advanced.

BigQuery procedure job, location europe-west3:
`airflow_cocoa_daily_run_bq_pipeline_2026_10_27T20_32_44_00_00_49cd90eb95c4cfb6c240fccdb73582ca`.

This proves one controlled production cycle. Recurring source-quality gates,
atomic all-product publication, chronological forecasting evaluation, sustained
service targets and rendered-report acceptance remain later checklist work.
Historical warehouse replacement/truncation was not performed. The accepted
source-history differences remain documented in [migration evidence](eia-migration.md).

## Serving identity and recovery evidence

Serving function/entry point: publish_eia_data; trigger: eia-oil-trigger;
output: oil-prices-topic; credential: Secret Manager EIA_OIL_API_KEY.
The validation topic/subscription remain available for isolated testing, but
the serving function currently publishes to production.

The validated source bundle contains exactly main.py, requirements.txt and
oil_schema.avsc. SHA-256:
`a020ce805a64b2a909b7489187d1b003ac1d79444d06ca0950b800bbdc84da34`.
Its serving archive members matched reviewed source and canonical schema.

Installed DAG SHA-256:
`c65aa41e2eee9cd417cfecd966d77e27da5f068eb1a21371db4fd3855922a8fc`.
Previous DAG SHA-256:
`40265cf6a43faaa2378e1a17020d2ef59be27d781bf3bc81cd521473c7b9b1ff`.
VM recovery directory: `/opt/airflow/releases/eia-63cb0f7/`, including original
and serving DAG bytes, approved preflight state and completed-cutover.json.

Private local evidence under `.build/eia-migration/` includes source/config/IAM
backups, before/after state, reviewed plans/logs, the completed VM receipt,
and production-output-validation.json with query jobs/results. The previous
function source backup, legacy-function-source.zip, has SHA-256
`185a68a7b909572c22acfd4a0d75119af6e8bceba13e28fc438b78a8e8c80d6d`.
Its actual identifiers and deployment settings remain in the private metadata
backup and unchanged [baseline resource record](https://github.com/IsaacOnKeys/Cocoa_Prices/blob/0236abaad185d1f6b475aa6f4c4a105058c95aad/docs/infrastructure/gcp-resource-inventory.json).
The previous credential is retained; its value was not read during retirement.
No secret versions are managed in Terraform state.

## Rollback after retirement

Pause the DAG and verify no active run. Disable EIA invocation before enabling
any replacement production producer. Recover the checksummed previous source
bundle and metadata, recreate its function/trigger with the retained credential
and restore its service-scoped Eventarc invocation grant. Restore dag_before.py
with its recorded ownership/mode and reconcile the trigger Variable if present.
Re-adopt restored resources into reviewed Terraform configuration/state; do not
remove unrelated lifecycle guards. Verify exactly one production producer and
a complete controlled daily cycle, then restore the prior pause/VM state.

Recovery must account for messages already acknowledged and warehouse writes
already made. Restoring source does not reverse those effects. Reconcile them
through isolated, measured repair; never run an unrestricted historical
WRITE_TRUNCATE as part of producer rollback. Credential deletion and historical
warehouse replacement were outside this cutover approval.
