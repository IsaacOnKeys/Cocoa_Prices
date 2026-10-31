# Existing two-page Looker Studio report

Checklist item 35 is in progress. Report ID: 0458444b-cbb3-4adf-afad-6e9b2ebb93ff. The owner supplied both editor URLs on October 31. Neither page could be opened with this session's web reader, and there is no connected browser/editor. The page names, charts, current data sources, aliases and credentials have not been inspected or changed. The mappings below are a proposed configuration, not a claim about the existing report.

The versioned field/query contract is [bi/looker_studio_report.json](../../bi/looker_studio_report.json). It reuses six existing accepted-release serving views; no new warehouse copy, pipeline, model, BI Engine reservation, paid connector or subscription is required.

## Page BGEqE: market and data quality

Open [page 1](https://datastudio.google.com/u/0/reporting/0458444b-cbb3-4adf-afad-6e9b2ebb93ff/page/BGEqE/edit). Preserve useful existing charts after inspecting their sources; replace obsolete fields individually.

| Display | Source and configuration | Meaning |
| --- | --- | --- |
| Release/status banner, on both pages | product_delivery_status; filter product=bi_serving; display delivery_status, status_message, accepted_business_date, original_publication_time, release_id and checked_at | Current, overdue or unavailable at the weekday 20:00 Berlin deadline. Keep the actual dates visible; do not use chart rendering time as data freshness. |
| Latest cocoa price | ls_series_quality; measurement=cocoa_price AND is_latest=true; MAX(usable_value) | EUR/metric tonne. Null stays unavailable; never replace with zero. Pair with original_value_date and quality_status. |
| Cocoa history | Same source; date dimension ascending; measurement=cocoa_price; MAX(usable_value); default last 90 days | Daily calendar series, including explicitly carried observations. No sum of prices; no smoothed or connected lines across missing values. |
| Brent history | Same source; measurement=oil_price; date ascending; MAX(usable_value); same date range | **USD/barrel**, not EUR. The legacy brent_price_eu name does not indicate currency conversion. Keep a separate axis/chart from cocoa. |
| Measurement quality table | ls_series_quality; is_latest=true; measurement, quality_status, original_value_date, latest_measurement_age_days, latest_measurement_overdue, preserved_prior_value | Includes cocoa, Brent, precipitation and soil moisture. Show observed/carried/missing/expired and missing revisions that preserved prior values. Do not imply that a carried reading was observed today. |
| Source acquisition table | ls_freshness; feed, last_ingestion_time, hours_ago, source_name, source_url | Supplier acquisition freshness differs from observation date and release freshness. Keep attribution links. |

Group the date control with history charts only. It must not filter away the latest-value cards, freshness/status tables or page 2 forecast because their dates have different meanings. Show currency/unit in each relevant title. Weather quality is included without introducing unverified weather-unit conversions or a blended cross-source plot.

## Page p_m0h69dttwd: forecast and evaluation

Open [page 2](https://datastudio.google.com/u/0/reporting/0458444b-cbb3-4adf-afad-6e9b2ebb93ff/page/p_m0h69dttwd/edit).

| Display | Source and configuration | Meaning |
| --- | --- | --- |
| Forecast price card | current_forecast; MAX(forecast_cocoa_price); unit EUR/metric tonne | One **40-calendar-day-ahead cocoa forecast**, not a 40-point future curve. |
| Forecast context table | Same source; origin, issued_at, feature_cutoff, target_date, model_horizon_days, model_id, forecast_product_version, release_id | Actual issue time is separate from feature origin and target. Show legacy product version if present; never relabel it as the new contract. |
| Issued-forecast comparison table | issued_forecast_metrics; method; MAX(issued_count), MAX(scored_count), MAX(mae), MAX(rmse), MAX(mape_percent), evaluation_status | Compare model, persistence and weekly-seasonal baseline on the same matured issued cohort. MAE/RMSE are EUR/metric tonne; mape_percent is already 0–100, so use a number with % suffix, not another x100 conversion. |
| Model status table | model_health; serving_model_id, serving_release_id, scored_forecasts, performance_status, retraining_policy | The model is provisional without enough matured outcomes; retraining is manual only. Do not advertise superior accuracy. |

When scored_count=0, show insufficient_matured_forecasts and leave accuracy metrics blank. Null does not mean perfect prediction. Retrospective model rescores and the local historical simulation are separate evidence; do not put them in the issued-forecast accuracy chart. Current forecasts and evaluation tables have fixed scopes and do not inherit the historical price date control.

## Ownership, access and refresh

Metric owner: project operator. These definitions and contract version 1.0 govern the report. Additive fields preserve compatibility; changing units, grain, forecast horizon, cohort or formulas requires a new contract version and a reviewed report update. Prices are daily levels, not revenue or additive measures. Release and model IDs are audit context, not numerical metrics.

Initial access should remain restricted to the owner and explicitly selected reviewers. Proposed simplest credential mode is the existing report owner's credentials, using the Cocoa project as the BigQuery billing project. This permits authorized report viewers to see the report's data through those credentials: do not make a report or reusable data source public as a shortcut. Do not grant an invented Codex email, broad editor roles, raw/staging access or secret access. The actual account, current sharing and underlying view access must be inspected in the editor before acceptance; no IAM change is applied by this package.

Use the free BigQuery connector. Propose a 15-minute data freshness interval, no unattended auto-refresh, and a manual refresh of all sources after the daily release when checking delivery. Refresh/caching can delay status and different sources can cache different releases; show their release IDs and refresh all sources if they disagree. This is not a transactional guarantee across rendered charts. Do not claim live status when checked_at is old. Actual settings need editor confirmation. [Google's freshness documentation](https://docs.cloud.google.com/data-studio/manage-data-freshness) explains shared owner-credential caching; [credential documentation](https://docs.cloud.google.com/data-studio/data-credentials-article) explains delegated data visibility.

## Prepared verification and cost evidence

```powershell
.build/python311-ops/Scripts/python.exe scripts/diagnostics/validate_bi_sources.py --output .build/bi-preflight-new.json
```

This checks required fields on the six existing views and submits seven BigQuery **dry runs**, never executed SELECTs. On October 31 all six schemas matched and all seven probes compiled. Estimated processed bytes were 9,296,027 for the 90-day market query, 9,659,171 for latest quality, 452 for status, 21,988 for source freshness, 26,813 for current forecast, 2,898,520 for metrics and 2,898,893 for model health. These are optimizer estimates for representative SQL, not billed bytes, the exact SQL generated by Looker Studio, or a monthly report bill. No cloud compute was started, report edited, model trained or data query executed for this preflight.

The script also emits connector links, optionally using --links-only without credentials. Each link opens a separate draft with one configured source; it does not update the existing report or create the specified charts. Existing report source aliases are unknown, so no misleading template-remapping link is supplied. Never interpret a generated link as a saved report deployment.

## Required browser acceptance

1. Inspect both existing pages, source aliases, field types, credential owner and sharing; record their actual state before editing. Preserve the owner's working layout wherever possible.
2. Connect the six approved views and apply the individual chart settings above. Confirm cocoa EUR/tonne and Brent USD/barrel; no price SUM, unwanted date aggregation or conversion of nulls to zeros.
3. Reconcile a latest cocoa value, a Brent value, the active release ID, one carried observation and the forecast price/origin/target/model against accepted warehouse outputs with a small explicitly capped query. Check duplicate-grain counts before relying on MAX to display one value.
4. Verify both rendered pages, date filters, empty ranges, null gaps, fixed-scope cards, source links and Berlin dates near a day boundary. Confirm unavailable/overdue messaging using isolated fixtures or a copy, without changing production release state.
5. Check that forecast accuracy remains blank when no issued outcomes have matured. Verify no forty-point legacy curve or retrospective accuracy claim is presented as the current product.
6. Open as the intended reviewer and as an unauthenticated visitor to establish the intended access boundary. No public sharing change is authorized merely by these instructions.
7. Measure one cold two-page load, a cached revisit and one filter change using actual BigQuery jobs, recording report ID, job IDs, cache hits, processed/billed bytes and duration. Distinguish development from recurring use. Use those results to close the remaining item 34 dashboard-cost check; dry-run estimates alone do not satisfy it.

Items 34 and 35 remain open for the actual report cost/visual/access evidence. Browser control of an authenticated owner session is the remaining editor dependency; no passwords, cookies or API keys should be pasted into chat.
