# Production service and data-product contract

Contract version: 1.0. Defined 2026-10-27 for checklist item 1.

The owner accepted weekday delivery by 20:00 Europe/Berlin, a 99% on-time
target over 100 operating days, an acquisition-loss limit of one operating
day, and restoration by the next operating day's deadline. The owner also
requires the entire release to be withheld when forecasting fails.

This document defines the target behavior. The current runtime does not yet
enforce these requirements; implementation, deployment and operational
evidence belong to the subsequent checklist items. No achieved service level
or externally guaranteed SLA is asserted.

## Users, products and ownership

The primary user and accountable operator is Isaac, the repository owner.
The initial consumer is the project's existing BI report and its intended
readers. Operational notifications must reach Isaac; the delivery address and
notification-channel setup are implemented and verified under checklist item
26, rather than inferred from a Git author email.

The product supports exploration of historical cocoa prices, oil and weather
context, and explicitly qualified forecasting results. It does not establish
causality, trading suitability or forecast accuracy merely by publishing data.

One accepted release contains:

| Product | Contract |
| --- | --- |
| Curated source series | Traceable, validated ICCO cocoa, EIA oil and NASA POWER weather observations; distinguish source observations from calendar-filled values. |
| Daily analytical series and features | Consistent daily grain, units and source/as-of metadata; explicit missingness, imputation and value age. |
| Forecasts and evaluation | Validated, consistently labeled forecast outputs, issued/model/input cutoffs, and an honest evaluation status; current historical rescoring must not be called chronological out-of-sample performance. |
| BI serving outputs | Stable, documented outputs referencing the same accepted release, with release time, source coverage and quality/staleness status visible to consumers. |

The current forecasting implementation uses a fixed 40-calendar-day horizon
from each feature row. Choosing a different forecast product belongs to
checklist item 22; this contract does not silently redefine it as forty
horizons issued from today's information.

## Operating calendar and delivery

All scheduling and business-date decisions use the IANA timezone
`Europe/Berlin`, including daylight-saving changes. Stored instants use UTC.

| Setting | Requirement |
| --- | --- |
| Operating days | Monday-Friday, including local public holidays unless a future version explicitly changes the service calendar. |
| Business date | The Berlin calendar date of the scheduled release; source observation dates retain the source's meaning. |
| Existing startup / acquisition / shutdown schedule | VM startup 18:30 daily; DAG trigger 19:00 Monday-Friday; VM stop 20:30 daily. These are source-recorded settings, not a fresh deployed inspection. |
| Delivery deadline | One accepted release for the business date visible to intended BI consumers by 20:00 Berlin time. |
| Non-operating days | Keep the last accepted release available with its real date/status; do not count a missing weekend release as a missed delivery. |
| Late delivery | Recover and publish a validated release when possible, but retain the original deadline miss in reliability reporting. |

The existing schedules are defined in [the DAG](../../airflow/dags/cocoa_daily.py)
and [Scheduler Terraform](../../infra/terraform/scheduler.tf). Safe handling of
long-running work before shutdown and missed-run detection are required by
checklist item 27; the shutdown window must not silently override recovery.

## Mandatory feeds, completeness and freshness

All three feeds are mandatory. For each release, acquire and record a successful
response from each source in that business date's operating window, beginning
at 18:30 Berlin time, and bind the response to the release's input cutoff.
An old queued message, an increasing staging count or a repeated observation
does not independently establish a successful current source acquisition.

Reconcile the observations actually published in the acquisition/recovery
window against the accepted/quarantined records. Source-explicit missing
values must be accounted for and labeled; inventing an observation for every
calendar date is not completeness. Relevant source revisions must be handled
deterministically. Archive the availability evidence used by the decision.

Observation age is `business_date - observation_date` in calendar days. Use
the latest valid observation of each required measurement. These initial
maximum ages are conservative project limits, not upstream service promises:

| Source | Expected availability and latest-observation rule | Maximum age at release |
| --- | --- | --- |
| ICCO cocoa | Use the actual dated EUR/tonne daily-price observations exposed by ICCO, including its published market-closure notes. Require the latest valid exposed observation; do not assume today's quote is already available or substitute the scrape date. | 7 calendar days |
| EIA RBRTE | Require the latest valid observation available at the input cutoff, with API dates/values corroborated against EIA's published weekday table. Daily observation frequency does not mean daily publication; an unchanged quote can be valid before the next release. | 14 calendar days |
| NASA POWER precipitation and root-zone wetness | Require the latest valid values available for the configured point and both parameters, reconcile the acquired window, and preserve each measurement's date. Meteorological data normally has a 2-3 day latency; a current-date missing sentinel is not a current valid value. | 7 calendar days per measurement |

Source references checked on 2026-10-27:
[ICCO dated quotes and market-closure notes](https://www.icco.org/),
[EIA series and release metadata](https://www.eia.gov/dnav/pet/hist/RBRTED.htm),
and [NASA POWER latency guidance](https://power.larc.nasa.gov/docs/faqs/data/).
Scheduled release dates alone are not proof that the data has become available.

If a source response succeeds, its latest valid observation is unchanged,
availability evidence explains the delay, and the age limit passes, classify
the feed as `current_to_source` and expose the age/delay. This is permitted
lagged data, not a fabricated new observation. Do not report a source as
current when availability cannot be established, the acquisition failed, an
available required record is missing, or the age limit is exceeded.

Source holidays affect expected observations, not the weekday delivery
denominator. A same-day observation is not mandatory. Forward fill must retain
the original measurement date and cannot bypass these age limits. Tighten or
revise thresholds from measured coverage through a reviewed contract change;
do not loosen them automatically during an incident.

## Publication and failure behavior

Publication is all-or-nothing for the contracted release. All mandatory feeds,
data-quality gates, forecast validity/evaluation-status gates and BI serving
checks must pass against a recorded input cutoff before that release is
visible as current. Unexpected partial publication is a failed release even
if the orchestration reports success.

When acquisition, validation or forecasting fails:

- Withhold the entire candidate release, including otherwise valid new core
  analytics. This is the owner's selected failure policy.
- Preserve the previous complete accepted release. Its source dates and issue
  times remain unchanged; show a missed-update/stale status for the new
  business date. Never relabel old predictions as newly issued.
- Preserve raw inputs, failed-record evidence and the failed run outcome;
  retry/replay through controlled recovery and notify the operator.
- Distinguish source-unavailable, pipeline-failed, quality-rejected and
  forecast-failed outcomes so an operator can identify the required action.

`current_to_source` with documented, permitted observation lag is an accepted
feed status. Missing mandatory feeds, unverified data availability, invalid
forecasts and mixed-release outputs are not permitted degraded releases.

## Reliability, detection and recovery targets

For each operating date, record exactly one scheduled delivery outcome.
Success means the complete accepted release for that date is visible by
20:00. Retries and manual runs do not create additional denominator entries.

`on_time_delivery = successful_operating_dates / scheduled_operating_dates`

The target is at least 99% over the latest 100 operating dates: at most one
miss in a full window. Before 100 dates exist, show the actual numerator,
denominator and observation period; do not claim a fully demonstrated
100-day target. Upstream outages, source-age breaches, maintenance and model
failures count as misses if they prevent the contracted release. Never
retroactively exclude incidents or change a deadline to improve the metric.

Detect and notify actionable in-window failures within 15 minutes of their
recorded occurrence; report an absent/missed release by 20:15 Berlin time.
The deadline monitor must operate independently of the scheduled VM. A source
delay within its documented limits is informative metadata rather than a
pipeline incident.

| Recovery objective | Definition |
| --- | --- |
| RPO | At most one operating day's newly acquired input evidence may be irrecoverably lost after an incident. Reprocessing equivalent already archived inputs is recovery, not loss. Retention of source revisions is part of this evidence. |
| RTO | Restore the complete service and reconcile replayable gaps by 20:00 on the next operating date after incident detection; state the corresponding actual elapsed time, including weekends, in each recovery record. |
| Existing accepted outputs | Preserve the last accepted release and restore access to it; a failed new candidate must not erase it. |

These are application recovery targets, not guarantees that an unavailable
external publisher can be restored by this project. An external outage that
prevents delivery remains a recorded target breach. Source history that can
no longer be reconstructed must be recorded as an unrecoverable gap, not
silently synthesized.

## Acceptance and change control

Checklist item 1 is complete when this definition records the agreed targets,
mandatory products/feeds, source-aware limits, failure policy and accountable
operator. Enforcement and measurements remain open until their later checklist
items are implemented and verified.

Implementation evidence must identify the code/config version, source input
cutoff, quality results, forecast/model identity, publication outcome and
consumer-visible release. Source freshness examples must cover an ICCO
previous-day quote, an EIA observation unchanged until its next release, NASA
latency/sentinels, a real acquisition failure and a source-age breach.

Changes to service objectives or the all-or-nothing publication policy require
the owner's explicit acceptance. Technical threshold changes need a reviewed
rationale and fixtures. Keep contract history in Git; record production
validation separately from local checks. Require 30 operating dates of
scheduled evidence for the initial readiness review, while reporting the
100-operating-date reliability target as provisional until observed in full.
