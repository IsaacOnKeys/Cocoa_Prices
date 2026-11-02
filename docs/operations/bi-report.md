# Cocoa Looker Studio report

Report: [Swiss Cocoa Price Predictions](https://datastudio.google.com/reporting/0458444b-cbb3-4adf-afad-6e9b2ebb93ff). Updated in the authenticated owner editor on 2026-11-01, on branch codex/governed-bi-report. A named recovery version, `Before governed BI update - 2026-11-01`, precedes the edits. The machine-readable field/query contract is [bi/looker_studio_report.json](../../bi/looker_studio_report.json).

Consumer review on 2026-11-02 identified date-picker usability concerns and a real oil-history gap. Final acceptance remains open. The original 5 GiB/user/day quota remains the cost guardrail.

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
- Both charts use Line Breaks for missing values. Prices and indices use MAX, never SUM. The page date control applies to both history charts. Conflicting groups were removed; the acquisition-freshness table has no date-range dimension, and the other page's fixed-scope cards remain independent.
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

Earlier verification was blocked by the 5 GiB/day per-user query quota: job 3197e642-e67a-440f-b67f-900645fa471a returned QueryUsagePerUserPerDay quotaExceeded on November 1. The view deployment succeeded (job 0e6ba1c7-9172-4d95-b8ef-8acce6266889), but the failed comparison was not semantic validation. The original grouping also left the cocoa chart outside the date control's scope. These issues were resolved and verified on November 2 as recorded below.

The extension refused an isolated browser context (Target.createBrowserContext: Not allowed). Anonymous access was subsequently verified in a separately launched disposable Chrome context with no account state; the signed-in session alone was not used as proof. No login tokens, cookies or browser credentials belong in the repository.

Changed-file Ruff, Python 3.11 compilation, JSON parsing and git diff whitespace checks passed. No containers, pipeline, existing completed feature tests, VM startup or model training were used for this BI change. Private receipts are development evidence, not proof of sustained delivery or a normal monthly bill.

A November 1 retry was also quota-rejected (job 0c0cb579-0385-4899-9984-3a821eb1e473). Non-executing dry runs estimated 9,296,027 processed bytes for each historical chart; this established SQL/source compatibility, not rendered acceptance. The generic dataset-fetch errors seen then cleared during the November 2 verification.

## Final technical acceptance: November 2

- With explicit owner authorization, the per-user quota was temporarily increased from 5120 to 6144 MiB. The project cap stayed 65536 MiB. The original 5120 MiB was restored and read back as effective. Restoration required the same percentage-decrease acknowledgement already configured in Terraform; the below-usage safety check remained enabled. Job 9ac99e51-6479-451a-86c4-e83499c63997 then successfully queried all three freshness rows under the restored limit, billing 20 MiB. No alternate billing project or query identity was used.
- Job 4cf2f380-1e04-4878-9a3b-25fb624cdc1b compared the history and index views with accepted measurements: 4685 rows, 4685 unique dates, zero mismatches, zero missing readings turned into values and zero indices outside their defined ranges. It processed 9,296,027 bytes and billed 70 MiB.
- Both history charts responded to October 26-November 1. A November 2 range correctly plotted no observations in either chart; no dataset error or fabricated zero-price line appeared. Source acquisition freshness remained visible. The KPI page retained the October 29 observation, October 30 origin, December 9 target and zero matured outcomes while the other page had an empty range. Default filters were restored; the freshness title now explicitly says Source acquisition freshness (UTC).
- All eight active embedded sources were read back at 15-minute freshness. No scheduled delivery, Pro subscription or BI Engine reservation was enabled.
- Fresh signed-out Chrome contexts rendered both public pages with zero edit controls and zero dataset errors; the public history page rendered both full-history charts and source attribution. German locale showed 5.013,48 and 5.025,53, equivalent to the English-format prices; dates localized consistently. This was a browser-locale difference, not a data mismatch. Both owner pages were visually inspected and captured in .build/item35.
- Final report-labelled jobs measured a mixed-cache two-page visit at 390 MiB billed: history 160 MiB, KPIs 230 MiB across 13 jobs, including six query-cache hits. The corrected seven-day change added one uncached 70 MiB price query because the driver query was reused; an empty-range change billed 140 MiB. The cached revisit interval issued no new BigQuery jobs. These are observed bytes, not a fully cold worst-case estimate or a euro invoice. Job IDs and time windows are retained in final-nov02.json and report-cost-summary.json.
- No VM startup, container build, source acquisition, pipeline rerun or model training was needed. The actual scheduled evidence is recorded in [the operational handoff](operational-handoff.md); one scheduled success does not demonstrate the 100-operating-day service target.

## Consumer review corrections: November 2

The date picker was tested in a fresh, signed-out Chrome session using ordinary
pointer clicks, including the year/month selectors, separate start and end dates,
and Apply. Both charts displayed January 1-31, 2025; their axes showed only that
interval. The source table remained visible. No forced clicks or synthetic DOM
click events were used in this public custom-range check. The connected owner's
background tab had animation/stability problems, so it was not used as the proof.
Receipts: `.build/item35/custom-range-result.json` and matching screenshot.

In View mode, open the top-right date control, click each
calendar's month/year heading to select its year and month, choose the start and
end days, then click Apply. In German these controls read Zeitraum and Anwenden.
The owner clarified that choosing the start/end dates was the difficulty. The
page header now says: Historical prices | Dates: choose start and end above,
then Apply. The test established working selection; the new hint addresses its
discoverability rather than claiming a calendar software defect was reproduced.

Brent remains the correct commodity after the supplier migration. The official
[EIA series definition](https://www.eia.gov/dnav/pet/hist/RBRTED.htm) is Europe
Brent Spot Price FOB, series RBRTE, USD/barrel. The legend now identifies EIA,
Brent spot, original units and the 0-100 index. The driver title also identifies
Brent spot USD/barrel. The plotted index is not itself a dollar price.

The previous history reconciliation established agreement with accepted data,
not completeness of supplier coverage. A bounded follow-up query found no
accepted oil source observations from September 23, 2025 through August 10, 2026.
After the 14-day carry expired, October 7-August 10 correctly remained NULL
(308 calendar days). The held EIA archive contains 223 actual prices in the
source gap, with its manifest SHA256 verified against the local archive.
The repair uses the existing isolated Beam candidate and reviewed atomic-release
workflow; it must not interpolate prices or independently patch serving tables.
Consumer acceptance remains pending correction and publication evidence.

The local Python 3.11 / Beam 2.65.0 run completed with 218 valid records and zero
rejections. The normal prepare command verified candidate ownership/counts and
source generation/hash; its preview showed 218 insertions, zero changes and zero
unchanged dates. Every proposed price was compared exactly with the protected
archive. Repair `c98b6cebbff9dfcc6ae3a8e485c4b1922bbafda00dcba257435e9df59a976d56`
is approved in the existing ledger for the next normal atomic release. This is
queued, not published. The interval is September 23, 2025-August 3, 2026, wholly
older than 90 days. The subsequent August 4-10 interval remains source-missing;
the unchanged 14-day carry can bridge it from the August 3 observation to the
next real observation on August 11. No current observation, existing price,
model, acquisition watermark or carry policy was changed. No VM or Dataflow job
was started; the warehouse load used an isolated candidate table.

Run/plan receipts are `.build/item35/oil-gap-run.json` and `oil-gap-plan.json`.
The completed manifest records Beam state DONE, exact code/source hashes and
counts. The Windows PowerShell wrapper reported a nonzero status after native
stderr deprecation warnings, with no Beam traceback; candidate completion and
its exact contents were independently checked by prepare and plan review rather
than treating the wrapper status as a clean process pass.

The header hint, precise driver title and Source acquisition freshness (UTC)
title were read back after a report reload. Final oil-gap rendering still needs
verification after the next accepted release consumes this repair. Step 35 and
final step 36 acceptance remain open until that evidence and consumer review.
The owned temporary candidate dataset was removed after approval; the immutable
source, rejection evidence and reviewed ledger plan remain. No recurring
candidate-storage resource is retained. The source table's original vertical
layout was restored after editing its title.
