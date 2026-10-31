# Recovery and idempotency evidence

Checklist item 30 combines existing native receipts with one credential-free handoff check. It adds no cloud services or recurring cost. Do not rerun paid training or every prior drill to reproduce this evidence.

| Failure | Evidence and result | Evidence level |
| --- | --- | --- |
| Upstream outage | [Retry delivery](retry-delivery.md) and acquisition-coverage tests: bounded retries; incomplete acquisition withholds publication; next acquisition expands the requested window. | Earlier native source/transport probes plus local coverage checks |
| Sink failure | [Ingestion cutover](ingestion-cutover.md): denied sink returned retryable failure with zero rows; the original delivery succeeded after permissions were restored, while the VM was stopped. | Earlier isolated GCP drill |
| Poisoned record / recovery sink failure | [Dead-letter recovery](dead-letter-recovery.md): export permission denial, repair, original-byte export, bounded replay and accepted remediated delivery; temporary resources removed. | Earlier isolated GCP drill |
| Consumer death after sink write, before ACK | New `test_recovery_handoff.py`: process exit at the real handler's accepted-log boundary, followed by original-message redelivery and the unfinished second record. | Local fault injection; cloud RPCs replaced |
| Duplicate delivery / restart during batch | Same check: append-only sink intentionally ignores insert IDs. Three writes after restart preserve two original revisions. The production oil freeze SELECT groups them to two facts. | Local component check; production SELECT executed through SQLite dialect adaptation |
| Outage beyond message retention | Same check: advance consumer clock eight days beyond acquisition, discard transport queue, verify retained batch hash/schema/identities, replay original Avro bytes through the real handler with new message IDs. Five total writes still group to the two archived observations, including the explicit NULL reading. | Simulated elapsed retention, not an eight-day GCP experiment |
| Interrupted publication | [Atomic release](atomic-analytics-release.md): failed freeze/publication rolls back, duplicate job reattaches, serving rollback is coherent. [Historical repair](safe-backfill-repair.md) adds stale-baseline and cancelled-plan rejection. | Earlier isolated BigQuery drills |
| VM/runtime loss | [Runtime backup and restore](runtime-backup-restore.md): encrypted metadata/images restored into isolation in 116.39 seconds; unfinished work neutralized and temporary resources removed. [Scheduling](safe-vm-scheduling.md) prevents old-day work from restarting publication. | Earlier native restore and runtime checks |

## Reproduce the new check

Use the Python 3.11 development environment, from the repository root:

```powershell
$recoveryTemp = Join-Path (Get-Location) ('.build/pytest-recovery-' + [guid]::NewGuid().ToString('N'))
.build/python311-dev/Scripts/python.exe -m pytest tests/unit/test_recovery_handoff.py --basetemp $recoveryTemp -q
```

The check calls the actual acquisition archive producer, archive verifier, replay publisher and push handler. Only storage, Pub/Sub and BigQuery IO are faked. The production oil SELECT is extracted from `sql/procedures/run_analytics_release.sql`; its table reference, parameters and timestamp cast are adapted to SQLite, retaining the selected columns and grouping. This verifies handoff and grouping semantics, not BigQuery execution or a complete publication. Existing native publication evidence supplies that separate level.

Reconciliation: two raw records; one write before process death; two writes on restart; two writes on retained-input replay; five physical rows, four delivery identities, two observation identities and two frozen facts. Original acquisition timestamps, values, missingness and observation identities survive. Physical staging duplicates are expected; accepted analytics has one fact per business revision.

## Operator recovery and limits

1. Keep the last accepted analytics release serving. Use the alert's acquisition/delivery IDs to inspect accepted receipts, quarantine and dead-letter evidence; do not infer loss from an unacknowledged publish alone.
2. Within transport retention, repair the failed dependency and allow redelivery. Do not purge the subscription or restart paid training to repair ingestion.
3. After transport expiry, locate the exact retained batch URI, generation and SHA in its acquisition receipt. Use [daily raw input recovery](daily-raw-inputs.md) to verify/export first, then explicitly replay into the intended authorized topic with an incident replay key. Reconcile the complete observation manifest against staging, including NULL observations and known duplicates.
4. Replay preserves old acquisition time. It cannot satisfy a new day's acquisition gate: `frozen_provenance` requires acquisition at or after that run's start. Reacquire current source data for daily publication; use the reviewed historical-repair path when appropriate. Never rewrite timestamps to make recovery appear fresh.
5. If runtime state is lost, restore the matching reviewed runtime/routine revision using the backup runbook. Resume only inside the bounded operating window and normal quality/forecast gates. Record any missed deadline rather than silently extending the VM window.

The service targets remain RPO of one operating day and recovery by the next operating day's deadline, with weekday delivery by 20:00 Europe/Berlin and 99% on-time over 100 operating days. These drills show recovery mechanisms, not measured sustained compliance or guaranteed recovery during supplier outages. A failure continuing beyond a deadline must be recorded as a missed target. Retention is configured; its full duration has not elapsed in testing.

Data never acquired, or whose retained evidence has expired, cannot be reconstructed by replay. Suppliers may revise or omit historical dates and do not supply complete historical publication vintages. The accepted EIA migration preserves the previous archive but excludes 28 prior-only dates from supplier-verified history; historical forecast simulations remain explicitly non-vintage-complete. These previously accepted limitations remain visible; no fabricated observations, publication times or retrospective availability claims are introduced.

Validation on 2026-10-31: one focused Python 3.11 component check passed. No cloud deployment, VM start, Dataflow job, model training or additional cloud query was performed for this item.
