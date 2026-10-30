# Operational alerts and budget notifications

Checklist item 26, branch `codex/actionable-alerts`. The owner supplied the
recipient and authorized a setup test on 2026-10-30. The actual address and
billing-account ID are in ignored local `infra/terraform/terraform.tfvars`,
not committed source. Cloud Monitoring owns the email notification channel.

## Independent watchdog

`check_analytics_delivery` is an authenticated Python 3.11 Cloud Run function
with minimum zero/maximum one instance, 256 MiB, fractional CPU, concurrency
one and a 60-second timeout. It runs outside the Airflow VM and cannot start,
stop or mutate that VM, publish data, train models or read secrets.

One OIDC-authenticated Scheduler job invokes it every 15 minutes, weekdays
18:00-20:45 Europe/Berlin. Checks are active from 18:45 until 20:30; earlier
and later ticks are inexpensive no-ops. There is one bounded Scheduler retry.
The active checks read the authoritative delivery view, VM state, run-start
logs and queue age. Each warehouse query is capped at 64 MiB and uses a
15-minute request identity to avoid duplicate submission on uncertain retries.

| Code | Severity | Operator action |
| --- | --- | --- |
| `vm_not_running` | ERROR | VM was not ready by 18:45; inspect Scheduler/startup, then use the startup runbook. |
| `run_missing` | ERROR | No current-day start or final manifest by 19:15; inspect orchestration scheduling. |
| `publication_blocked` | ERROR | A current-day run failed; inspect manifest task states, source ages and quality results. |
| `release_missing` | ERROR | No complete current-day release by 20:00; retain the previous release and recover the failing stage. |
| `release_late` | ERROR | A release recovered after 20:00; record the miss rather than erasing it. |
| `backlog_aging` | WARNING | Source/consumer messages are over five minutes old; inspect consumer errors and retry/DLQ evidence. |
| `monitor_failed` | ERROR | Monitoring itself could not verify delivery; inspect its permissions/API failures. |

The watchdog does not mistake a missing start log for a missing run when a
current-day completion manifest exists. Engineering-validation runs do not
establish service delivery. DST follows the Berlin timezone; local public
holidays remain operating weekdays under the existing service contract.

An authenticated `{"probe":true}` request exercises the real read path outside
the operating window with alert emission suppressed. It cannot inject fake
source/VM state or change the clock. This is for operator diagnosis, not a
scheduled delivery success. The Scheduler's normal target/body is restored
after the native setup probe.

## Routing, severity and acknowledgement

Two log-based policies route operational ERROR and WARNING events. They also
cover a failed final run, quarantined messages and platform failures of the
watchdog/Scheduler itself. The existing dead-letter forwarding and stalled
recovery-export policies now use the same owner-selected email channel.
Exhausted transport retries therefore retain their existing durable evidence
and gain notification routing without changing transport behavior.

Messages link to the [operations dashboard](https://console.cloud.google.com/monitoring/dashboards/builder/b5f39305-22ae-44ea-983e-ffb23c40de73?project=cocoa-prices-430315)
and runbook. Log-based policies extract the stable alert code; manifests carry
the full run/release/acquisition context. Code/message entries must be read
before recovery actions; no alert automatically deletes, replays or retrains.

Repeated notifications are limited to five-minute intervals. Incidents close
after 30 minutes without a matching event; repeated events can keep them open.
Acknowledge in Monitoring, add the incident/recovery note, and snooze during
active maintenance if repeated notifications are not useful. Acknowledgement
does not repair the pipeline. [Google's incident guidance](https://docs.cloud.google.com/monitoring/alerts/log-based-incidents)
describes closure/snoozing and alert limits. These settings target the agreed
15-minute detection window; one setup exercise does not prove sustained SLOs.

## Project-only monthly budget

The linked billing account's currency was verified as EUR before configuring
a **EUR 10/month** project-filtered budget. Actual-spend thresholds are **EUR 5,
8 and 10**, with an additional forecasted EUR 10 threshold. The budget excludes
credits so promotional credits do not hide underlying workload charges. Other
projects in the billing account are excluded, and default IAM-recipient emails
are disabled; notifications go to the explicitly chosen operator channel.

This configuration sends notifications; it is **not an enforced spending cap**.
Billing data/notifications can lag and are not an invoiced-total guarantee.
Current actual account spend remains unverified. Broader measured cost and
workload guardrails remain item 34. See [budget recipient guidance](https://docs.cloud.google.com/billing/docs/how-to/budgets-notification-recipients).

No always-on VM or paid custom metric collector was added. There is one small
scale-to-zero function, one Scheduler job and two log-based alert policies.
Queries are bounded, inactive ticks skip reads, and existing source functions,
Avro contracts, Beam and daily model behavior are unchanged. The operating
target remains under EUR 5/month with EUR 10/month maximum including validation.

## Deployment and validation

Package only `monitor` with the existing deterministic function packager.
The provider copies function sources to a canonical `function-source.zip`;
the serving archive's bytes were verified against the tested ZIP. Terraform
pins the immutable upload generation; the provider's canonical copy/generation
is recorded separately. Do not replace that source pin merely because the API
returns its copied filename, and do not rebuild existing functions
to deploy this monitor. Scoped IAM grants allow only the required read APIs,
query submission and read access to the delivery/serving datasets.

Eleven focused Python 3.11 checks passed for calendar boundaries, absent runs,
stopped VMs, failed/late publication, backlog severity, typed query results,
bounded retry identity and packaging. Changed-file Ruff and Terraform validation
passed. Only the named new alert/watchdog/budget resources and related existing
recovery/dashboard routing were targeted; no full pipeline or earlier-feature
test suite was rerun.

Native results and the outstanding human receipt confirmation are recorded
below. Artifacts remain in `.build/actionable-alerts/`.

On 2026-10-30 the deployed Python 3.11 revision
`check-analytics-delivery-00001-yod` rejected an anonymous request (HTTP 403).
The Scheduler service account's authenticated probe completed at
21:05:18 UTC with the VM `TERMINATED`, delivery status `current`, no sampled
queue series and no emitted alerts. The original Scheduler target/body was
restored. This verifies the independent read path, not deadline detection on
an actual failed operating day. A targeted, read-only Terraform refresh found
no source drift; the deployed archive matched the tested package SHA256
`6e61fed7549f8f69ac7f7e5f5df0ca430a4765baeb18154bb3d8f5cb01ca6b0d`.

The authorized setup test evaluated a missing run/stopped VM and submitted one
clearly labeled `setup_test` event at 21:00:25 UTC. Monitoring opened incident
`0.oefshcrhjokg` under `Cocoa: operational critical` at 21:03:07 UTC, about
2 minutes 41 seconds later. No production outage was injected. **The owner
confirmed email receipt on 2026-10-30, completing item 26's setup acceptance.** The live
budget API separately verified EUR 10, the single-project filter, all four
thresholds and the selected notification channel. This did not measure actual
spend or trigger a billing-threshold notification.
