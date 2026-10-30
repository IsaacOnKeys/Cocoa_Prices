# One operational dashboard

Checklist item 25 uses the existing [Cocoa analytics operations dashboard](https://console.cloud.google.com/monitoring/dashboards/builder/b5f39305-22ae-44ea-983e-ffb23c40de73?project=cocoa-prices-430315).
Its Terraform identity and existing dead-letter charts are preserved.

The dashboard combines:

- Structured run start/completion entries: run/release/manifest IDs, task states,
  source acquisition IDs, coverage counts, observation ages and duration.
- Producer/consumer request rate, 5xx errors and p95 latency from existing
  Cloud Run metrics, plus structured quarantine/retry and application-error logs.
- Staging/trigger backlog and oldest-message age; dead-letter forwarding and
  recovery-export backlog/age from existing Pub/Sub metrics.
- Existing VM CPU and Ops Agent memory metrics, with the expected scheduled
  uptime made explicit. Empty charts are not proof of health.

There are no new application services, log-based metrics, custom metric
collectors or polling jobs. Existing producer/consumer JSON logs and transport
metrics are reused. The enabled, active Ops Agent's existing empty override is
captured in [the managed configuration](../../infra/ops-agent/README.md); its
collection settings and service were not changed.

## Diagnosis without SSH

1. Read the latest run outcome/source coverage entry. `task_states` identifies
   a failed producer, required-feed gate or release task. `run_started` without
   a completion is not success; external absent-run detection is item 26.
2. Check request failures/quarantine/retry evidence and queue age to distinguish
   upstream acquisition, consumer/sink and dead-letter export problems.
3. Use the authoritative warehouse view for accepted delivery state and exact
   quality results:

   ```sql
   SELECT * FROM `cocoa-prices-430315.cocoa_related_studio.operational_status`;
   ```

   It combines the accepted release and deadline status with the latest durable
   manifest from the last seven Berlin dates. No recent manifest is NULL,
   rather than an invented successful run. Ages are explicitly **at release**;
   current receipt traffic never refreshes an old accepted output's dates.
4. Follow the manifest/job IDs into retained records and BigQuery job details.
   To correlate a feed, use `jsonPayload.acquisition_id="<accepted acquisition>"`
   in Logs Explorer. The model-health view separately shows provisional
   performance and reviewed selection; operational success does not imply skill.

The `cocoa-operations` log uses JSON payloads and explicit INFO/ERROR severity.
Writes have a ten-second HTTP timeout, one call per event and sanitized failure
logging. A logging outage does not roll back accepted analytics: the durable
manifest and local task logs remain the recovery evidence. Source payloads,
credential values, connection details and SDK error bodies are not copied into
central events. Engineering-validation entries are excluded from the main run
panel. A manually observed retained run is labeled `retained_run_observed`,
not presented as a log emitted at its original completion time.

## Deployment, cost and validation

Render/deploy `operational_status_view`, install the telemetry-enabled DAG, and
apply the changed `google_monitoring_dashboard.dead_letter_recovery` definition.
No routine/function/Beam deployment or dependency rebuild is needed. Existing
VM startup/shutdown ownership is preserved.

Dashboard configuration adds no metric collection. Console metric reads are
not charged, while API reads, existing agent ingestion and log storage follow
[Google's observability pricing](https://cloud.google.com/products/observability/pricing).
Central events are small and only emitted at run boundaries; this is not a new
per-record logging stream. The budget target remains under EUR 5/month, maximum
EUR 10/month including validation; actual account spend has not been verified.

Five focused Python 3.11 checks, changed-file Ruff, Terraform formatting and
configuration validation passed. A targeted plan with refresh disabled updated
only the existing dashboard, with 13 widgets. Existing alert policies were not
changed and no full pipeline/earlier-feature test suite was rerun.

Native configuration and metric checks accepted all ten chart filters.
Staging/trigger queues, request rate/latency and VM CPU/memory returned sampled
points. Recovery and 5xx charts had no sampled series in the inspected window;
absence is reported explicitly, not converted into a health assertion. The
operational-status read billed 30 MiB and showed current delivery and the last
scheduled run's successful manifest, including actual source ages/counts.
Console rendering/human visual review is distinct from these API checks.

Native Airflow import and a central write passed using its real runtime
credentials. Cloud Logging returned the exact entry by insert ID, received at
2026-10-30 19:11:02 UTC. It accurately labels observation of the retained
scheduled run, whose manifest recorded completion at 18:03:36 UTC. No new
acquisition, release or training run was triggered for this check.

Installed DAG SHA256:
`987f654bbdd48542747d41cea35e6f3aaec4c5b5569c9a38fefd19923281c56a`.
The prior file is retained at
`/opt/airflow/releases/operational-telemetry-20261030/cocoa_daily_dag.py.before`.
The VM's existing scheduled running state was preserved; no new startup or
collector configuration was introduced. Receipt:
`.build/operational-telemetry/central-log-received.json`.

Artifacts are under `.build/operational-telemetry/`; the installed agent status,
targeted Terraform plan/apply, metric samples and warehouse snapshot are retained
there. Notification routing and delivery tests remain item 26.
