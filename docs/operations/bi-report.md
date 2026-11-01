# Cocoa Looker Studio report

Report: [Swiss Cocoa Price Predictions](https://datastudio.google.com/reporting/0458444b-cbb3-4adf-afad-6e9b2ebb93ff). Updated in the authenticated owner editor on 2026-11-01, on branch codex/governed-bi-report. A named recovery version, `Before governed BI update - 2026-11-01`, precedes the edits. The machine-readable field/query contract is [bi/looker_studio_report.json](../../bi/looker_studio_report.json).

## Release and 40-day forecast (BGEqE)

The original four-card layout is retained. The obsolete 14-day forecast and retrospective percentage-error claim are removed.

| Component | Accepted serving source and calculation |
| --- | --- |
| Latest cocoa, EUR/metric tonne | ls_series_quality, measurement=cocoa_price and is_latest=true, MAX(usable_value); the small date is original_value_date, displayed as a date dimension rather than a count |
| Forecast origin | current_forecast.forecast_origin_date, date dimension |
| One 40-calendar-day-ahead price, EUR/metric tonne | MAX(current_forecast.forecast_cocoa_price), paired with target_date as a date dimension |
| Matured forecasts | MAX(model_health.scored_forecasts); subtitle Issued outcomes only |
| Delivery status table | product_delivery_status, product=bi_serving; delivery_status, accepted_business_date, original_publication_time, status_message, checked_at; timestamp labels explicitly UTC |
| Forecast context table | current_forecast: origin, target, issued_at, feature_cutoff, model_id, forecast_product_version, release_id; title labels the model provisional and timestamps UTC |
| Latest measurement quality | ls_series_quality, is_latest=true; measurement, quality_status, original_value_date, latest_measurement_age_days, latest_measurement_overdue, is_imputed, preserved_prior_value |
| Issued evaluation | issued_forecast_metrics: method, issued_count, scored_count, mae, rmse, mape_percent, evaluation_status; missing metrics display blank |

The four tables use the view's precomputed rows as dimensions with no additional numeric metric aggregation. MAE/RMSE are EUR/tonne; MAPE is already a percentage, with no further multiplication by 100. Model, persistence and weekly-seasonal baseline use the same matured issued cohort. No matured outcomes means unavailable accuracy, not perfect prediction. Historical simulations and retrospective rescores are separate evidence. Retraining remains manual and reviewed.

## Historical prices and sources (p_m0h69dttwd)

The existing time-series layout and source-attribution table are retained. These are native Looker Studio time-series charts, although their rendered DOM contains Vega frames.

- Cocoa history now uses ls_market_history with MAX(cocoa_price) by date, EUR/metric tonne. The embedded source alias ds2 is retained; its former physical-table connection was replaced with the accepted-release view, with no field changes.
- The driver chart uses ls_indexed_series_smooth with MAX of cocoa_idx, oil_idx, precip_idx and soil_pct by date. Cocoa and Brent use full accepted-history min/max scaling to 0-100. Precipitation uses a seven-calendar-row mean, log(1+x), then full-history min/max scaling. Soil moisture uses a seven-row mean multiplied by 100. AVG ignores missing readings; a missing current-day value stays missing rather than becoming a smoothed observation. Historical indices can change after a new accepted release changes the bounds; filtering the display does not recompute normalization bounds. They are descriptive comparisons, not comparable physical units or evidence of causation. Underlying Brent is USD/barrel, despite the legacy brent_price_eu field name.
- Both charts use Line Breaks for missing values. Prices and indices use MAX, never SUM. The date control is grouped with the two history charts; acquisition freshness and the other page's fixed-scope cards have independent scopes.
- ls_freshness retains ICCO, EIA and NASA POWER attribution links. Its timestamps describe source acquisition for the accepted release, not new observations, and display UTC. Observation ages are on the first page.

[sql/serving/bi_history.sql](../../sql/serving/bi_history.sql) versions both historical views. Render with `scripts/deploy/render_delivery_sql.py bi_history_views --output <new-file.sql>`. Deployment created only logical views; no source tables, retained releases or model were changed. The previous indexed-view SQL is retained privately in .build/item35/indexed-view-before.json.

## Ownership, access and refresh

Metric owner: project operator. Contract 1.0 governs units, grain, cohorts and horizon. Additive fields preserve compatibility; changing those meanings requires a reviewed contract/report revision. Release/model IDs are audit context. Daily prices are levels, not additive revenue.

The sharing dialog showed only the owner's named account and **Public / Viewer** link access. The owner explicitly confirmed on 2026-11-01 that public viewer access should remain for the CV portfolio. No sharing invitations, public-access changes or IAM grants were made. The eight embedded BigQuery sources use the signed-in owner's credentials. Public viewers can therefore see the report's served data through those credentials; raw/staging tables and secrets are not report sources.

Use the free BigQuery connector and 15-minute freshness for all eight active sources. No paid BI Engine reservation, Pro subscription, scheduled report delivery or unattended refresh was added. Freshness settings govern query reuse; they do not mean a new observation arrives every 15 minutes. Rendering time in the report footer is not source freshness. The status table exposes checked_at and original publication time; refresh all sources if release context disagrees. Rendered charts are not an atomic transaction across independently cached sources.

## Executed verification and cost evidence

Private screenshots, bounded warehouse receipts and report-labelled BigQuery job metadata are in .build/item35 (ignored). On November 1:

- The report showed cocoa 5013.48 with observation date October 29; forecast origin October 30, target December 9, price 5025.5271181383432 rounded to 5025.53; product single_40_day_ahead_v1. The active release was 5824ad9c522a68f129ae45e558be74b687f1319ff94f6c282bab26e47c990b8c. Those values reconciled to the warehouse.
- The quality view contained 18,740 unique date/measurement rows, four latest measurements and zero expired/missing rows with a non-null usable value. Latest cocoa was carried from October 29; Brent/weather were carried from October 27. All three evaluation methods had one issued forecast, zero scored outcomes and null accuracy. The bounded reconciliation job d2d86f9d-e263-453a-8436-30c3503c275a processed 9,685,646 bytes and billed 90 MiB; this is development validation, not dashboard usage.
- A manual first-page refresh at 17:35:45 UTC and second-page load at 17:36:59 produced 13 report-labelled query jobs: eight cache hits and 250 MiB billed. This was a mixed-cache visit, not a proven fully cold visit. A cached first-page revisit at 17:37:54 issued no new report-labelled jobs before the next edit. A last-seven-days filter at 17:42:12 produced three jobs, 40 MiB billed, before the subsequent historical-source correction. Final-source measurements must be recorded separately; earlier numbers do not price the corrected history views.
- BigQuery bytes include per-table billing minimums and are not a euro invoice. Public report traffic can increase owner-billed queries. Existing 64 GiB/project/day and 5 GiB/user/day query quotas, project budget emails, zero serverless minimums and VM shutdown controls remain. A normal month's total below EUR 5, maximum EUR 10, is an operating target requiring billing evidence.

The schema/dry-run helper remains available; it does not prove rendered acceptance:

```powershell
.build/python311-ops/Scripts/python.exe scripts/diagnostics/validate_bi_sources.py --output .build/bi-preflight-final.json
```

Acceptance is blocked by the existing 5 GiB/day per-user query quota: the final historical comparison job 3197e642-e67a-440f-b67f-900645fa471a returned QueryUsagePerUserPerDay quotaExceeded on November 1. No quota increase, alternate query identity or billing change was used. The two view definitions were deployed successfully (job 0e6ba1c7-9172-4d95-b8ef-8acce6266889), but this failed comparison is not semantic validation. The last successful empty-range check exposed different date scopes between the charts; the history group was rebuilt afterward and still needs rendered verification. Default filters were restored.

After quota capacity is available, run the bounded history comparison, verify both history charts respond to the same nonempty and empty ranges while freshness remains visible, read back refresh settings, measure the final-source two-page query usage and obtain the owner's consumer/access review. Do not close item 35 before those checks. The extension refused an isolated browser context (Target.createBrowserContext: Not allowed), so anonymous rendered access must not be claimed from the signed-in owner session. No login tokens, cookies or browser credentials belong in the repository.

Changed-file Ruff, Python 3.11 compilation, JSON parsing and git diff whitespace checks passed. No containers, pipeline, existing completed feature tests, VM startup or model training were used for this BI change. Private receipts are development evidence, not proof of sustained delivery or a normal monthly bill.
