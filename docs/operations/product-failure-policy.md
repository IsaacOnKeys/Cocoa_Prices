# Product failure behavior

Checklist item 18 follows the owner's existing all-or-nothing policy:

| Failure | Publication and consumer behavior |
| --- | --- |
| Acquisition/required-feed gate | Stop before candidate publication; keep the previous complete release. |
| Input/output quality rejection | Reject the candidate; keep the previous complete release. |
| Training, prediction or forecast validity failure | Withhold core analytics, forecasts and BI updates together. |
| Publication transaction failure | Roll back all serving writes; keep the previous complete release. |
| No previously accepted release | Show unavailable; do not substitute candidate outputs. |

The existing release procedure and Airflow dependency chain implement these
publication rules. A successful task must confirm a published release receipt;
the final summary depends on that task succeeding. This feature does not change
either component or rerun their completed cloud acceptance tests.

## Consumer status

The additive `cocoa_related_studio.product_delivery_status` view returns one row
each for `core_analytics`, `forecasts` and `bi_serving`. All share the accepted
release/model, original publication time, activation time, input cutoff and
delivery status. Failed attempts never refresh those dates.

- `current`: the accepted business date meets the latest elapsed weekday
  20:00 Europe/Berlin deadline. This describes delivery, not model accuracy or
  today's source observation date.
- `missed_update`: keep displaying the previous accepted release, its actual
  dates and the supplied overdue message. All three products have this status.
- `unavailable`: no published registry entry matches the active marker, or its
  date is in the future. Suppress values and show the unavailable message.

Before 20:00 a weekday's new release is not yet overdue. Monday before 20:00,
Saturday and Sunday require Friday's release. Local public holidays remain
operating days. Berlin daylight-saving changes follow the IANA timezone.
An early failed attempt can still recover before the deadline; immediate failure
diagnosis remains in Airflow/run records, rather than mislabeling the last
accepted product. On-time reliability measurement is separate: a late release
can restore `current` without erasing the original deadline miss.

Render this view independently using `product_status_view` in
`scripts/deploy/render_delivery_sql.py`. It reads only the small release-control
tables, adds no stored copies, scheduled queries or infrastructure, and requires
no pipeline/model rerun. Dashboard queries still count toward warehouse usage.
Report authors must display the status/date beside values; rendered-report
integration and visual acceptance remain checklist item 35.

Local validation exercises the actual view's calendar/status logic in SQLite
with translations for BigQuery date functions and array expansion. This is
logic validation, not execution by BigQuery. Deployment evidence is recorded
separately when available.

## Implementation status, 2026-10-30

Branch: `codex/product-delivery-status`. Python 3.11 local validation passed
17 focused tests and changed-file Ruff; Git whitespace validation passed.
Existing pipeline tests and cloud workloads were not rerun.

After the owner refreshed Application Default Credentials, BigQuery accepted
the view definition on 2026-10-30 at 14:55:15 UTC. The returned schema contains
all 11 expected fields. Evidence: `.build/product-delivery-status/deployed.json`.
Creation used the metadata API; no query or pipeline execution was needed.
Item 18 is complete; dashboard integration remains item 35.

The owner's updated scope is a CV demonstration: prefer small additions on
existing services, validate each feature at the end, and avoid revisiting
completed features without a direct dependency or demonstrated defect. The
total operating budget targets under EUR 5/month, with a EUR 10/month maximum,
including development cloud work (owner update 2026-10-30). No current billing
total has been verified; this view adds no scheduled
workload or stored data, and this change did not start a VM, query job, build
or model training run.
