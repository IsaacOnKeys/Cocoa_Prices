# One 40-day-ahead forecast per release

The owner selected this product on 2026-10-30 for checklist item 22:
`single_40_day_ahead_v1`.

Each accepted release produces exactly one predicted ICCO cocoa price in
**EUR per metric tonne**, for **40 calendar days after its Berlin business
date**. The feature origin is that business date; issue time is the actual
publication instant. The release's recorded input cutoff bounds the source
information used by both training and prediction. Source age and bounded
carry remain visible separately; a feature date does not imply that every
measurement was observed that day.

Example: a release for **2026-10-30**, published at 19:15 Berlin with inputs
frozen at 19:10, uses the October 30 feature row and predicts the price on
**2026-12-09**. Its input includes only that release's frozen observations;
unknown future weather, oil and cocoa observations are not inputs. Training
continues to exclude rows with unknown 40-day labels. Whether its historical
evaluation is chronological remains item 23, not a claimed capability here.

`model_horizon_days=40` is measured from `forecast_origin_date`. For a late
release, `days_from_issue_to_target` can be smaller; the system does not
backdate the actual issue time. Missing business-date features, zero/multiple
predictions, a different target, or an invalid price withhold the release.

Use `cocoa_related_studio.current_forecast` for the display contract: origin,
actual issue time, feature cutoff, model, target, model horizon, days from
issue to target, price, unit and product version. Pair it with the existing
delivery-status view for overdue/unavailable messaging. A report should show
one point/value labeled **40-day-ahead cocoa price forecast (EUR/metric tonne)**,
not a forty-day daily trajectory. Rendered report wiring remains item 35.

The stable `forecast_40d` table name/schema is retained; new releases contain
one row. Old accepted issues retain their original forty rows, models and
dates in the issued-history view. Before the first new release, the current
view selects the old release's business-date +40 point and labels it
`legacy_origin_40d_v1`. It does not relabel an old issue as the new contract.
New release quality metadata and run manifests record the selected product.

The change uses the existing model and services, with no extra training
cadence, model type, dependency or storage copy. Prediction scores one row
instead of forty. The budget targets under EUR 5/month, maximum EUR 10/month
including validation; actual account spend has not been verified.

Completed 2026-10-30 on `codex/single-40-day-forecast`. Twelve focused Python
3.11 checks passed for origin selection, missing/duplicate/legacy-curve
rejection, exact target date, null target and year-boundary handling. Changed
files passed Ruff and Git whitespace validation. The quality diagnostic's
forecast fixture/mutations now use one +40-day point; its existing cloud suite
was not rerun.

One bounded native prediction reused the October 30 accepted model and frozen
features. It produced exactly one December 9 prediction, identical to that
release's retained +40-day value (5025.527118138345 EUR/metric tonne). The check
billed 30 MiB; no model was trained and no serving values changed. BigQuery
accepted the new view and procedure; procedure deployment billed zero bytes.

The deployed procedure SHA256 is
`d7f269491ec732f1e6ce7e918417dfbf1c2ec8f3afaab2a9d7a2c8062697ef8c`;
the installed DAG pins it and has SHA256
`596e12cfd1b6ff3ff03a3a9f7a6d7bf6a73711439c6b238e6b35a2be0943de97`.
The prior DAG is `/opt/airflow/releases/single-forecast-20261030/cocoa_daily_dag.py.before`;
the prior procedure is retained in `.build/single-40-day-forecast/rollback-procedure.sql`.
The VM was restored to `TERMINATED`. Native DAG import remains deferred while
the existing scheduler container is stopped; first scheduled publication of
the new one-row product remains operational evidence to observe. Existing
accepted issues remain unchanged. Evidence: `.build/single-40-day-forecast/sql-deployment.json`
and `vm-deployment.json`.
