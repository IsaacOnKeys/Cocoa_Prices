# Operational ownership and acceptance

Owner and first responder: Isaac, the project operator. The approved operational
and budget email channel is configured in Cloud Monitoring. This CV project has
one operator; there is no separate on-call team or invented escalation contact.
If the operator is unavailable, retain the last accepted release and show its
actual status. Never substitute a successful status for an unverified run.

## Incident entry points

| Symptom or task | Action and existing runbook |
| --- | --- |
| Missing/late release or failed monitoring | Read the alert code, manifest and deadline state using [actionable alerts](actionable-alerts.md) and [operational telemetry](operational-telemetry.md). Recover the failed stage; retain the original deadline miss. |
| Supplier or required-feed failure | Inspect the accepted acquisition IDs and receipts using [required-feed completion](required-feed-completion.md), [bounded source requests](bounded-source-requests.md) and [EIA policy](eia-migration.md). Keep all-or-nothing publication. |
| Retry, backlog or dead-letter recovery | Use [retry delivery](retry-delivery.md), [dead-letter recovery](dead-letter-recovery.md) and [recovery demonstration](recovery-demonstration.md). Reuse retained identities; do not manufacture a new acquisition time. |
| Historical replay or correction | Use [safe backfill and repair](safe-backfill-repair.md). Beam is a manually invoked backfill tool; review scope and costs before a cloud run. |
| Deployment or data rollback | Follow [application deployment](application-deployment.md), [runtime layout](runtime-layout.md) and [atomic releases](atomic-analytics-release.md). Distinguish a source Git change from a shared cloud deployment. |
| VM/runtime loss or failed backup | Follow [runtime backup/restore](runtime-backup-restore.md) and [safe scheduling](safe-vm-scheduling.md). Preserve retained data/keys and return maintenance compute to its original power state. |
| Dashboard error, stale or inconsistent values | Follow [BI report evidence](bi-report.md). Check the accepted release, observation dates and source refresh separately. A rendering timestamp is not a new source observation. |
| Quota or spending alert | Follow [FinOps controls](finops.md). Target below EUR 5/month; limit EUR 10 including development. Stop discretionary work, inspect usage and keep existing shutdown/instance controls. Budget emails do not impose a monetary cap. |
| Model change | Follow [manual model policy](manual-model-policy.md). Training is manual and promotion reviewed; zero matured outcomes cannot demonstrate accuracy. |

For each incident retain its actual UTC start/end, affected business date,
release/run/manifest IDs, symptom, corrective action and recovery evidence in a
private receipt. Include operator-induced failures and late recovery; do not
rewrite publication times or erase missed deadlines after a successful retry.
Keep secrets and source payloads out of incident summaries and Git.

## Evidence and acceptance

Implementation completion and observed service reliability are separate.
The service target remains 99% on-time over 100 operating days, weekdays by
20:00 Europe/Berlin, at most one operating day of acquisition loss, and recovery
by the next operating deadline. Holidays remain operating weekdays. The earlier
30-operating-day handoff proposal is not an industry requirement and has not
been accepted by the owner. Do not hold finished code for a fabricated test
period or claim an unobserved period passed.

For the CV handoff, report the available evidence and actual sample size:

- [October 27 validation](release-validation-2026-10-27.md) records one controlled
  DAG cycle; it is not proof of routine scheduled delivery.
- [Recovery demonstration](recovery-demonstration.md) records exercised recovery
  mechanisms and their limits; it is not guaranteed future recovery.
- [Run manifests](run-manifests.md) and existing run-boundary telemetry retain
  outcomes for future scheduled evidence. No extra poller, VM uptime, daily test
  pipeline, training schedule or paid monitoring service is needed.
- [BI acceptance](bi-report.md) records the accepted October 30 release and
  November 1 report work. Final historical reconciliation and rendered checks
  remain blocked by the existing per-user query quota; this is not a passing
  validation result.

When reviewing scheduled delivery, select a stated start and end date and count
every elapsed Monday-Friday whose Berlin deadline has passed. Match scheduled
Airflow run IDs and durable manifests to the original accepted publication time.
Exclude engineering/fixture/manual runs from scheduled-success evidence, but keep
actual scheduled failures and missing runs in the denominator. Report on-time,
late, failed/missing and unverified days separately. A current active marker does
not prove historical availability or that the report rendered for a consumer.
Calculate an observed rate only from supported evidence, and state any missing
logs or unverified availability; do not claim the 100-day target was measured
from a shorter sample. Review retained rollback events when assessing availability.

Before closing step 36, the operator must accept the concrete report as useful
for its intended CV audience and acknowledge the documented remaining limits.
Long-term scheduled reliability, normal-month spending and genuinely matured
forecast performance remain ongoing measurements with their actual windows.
Neither a feature count nor the word "production" substitutes for that evidence.

## Current handoff state (2026-11-01)

Steps 1-33 have recorded implementation evidence and accepted limitations.
Steps 34-35 retain their final-source BI verification/cost checks. The ownership
and runbook portion of step 36 is prepared here; consumer acceptance and a
scheduled-evidence assessment are still open. No new claim of production
readiness or achieved service level is made. Existing Airflow 2 CV security
exception and provisional model status remain explicit in their runbooks.
