# 1. Curated data

The analytical workflow starts by aligning the historical and daily source
records into one daily series.

**Implementation:** [frozen inputs, source candidates and daily join](../procedures/run_analytics_release.sql).

## Inputs and outputs

| Input | Role |
| --- | --- |
| stream_staging.cocoa_prices | New cocoa observations |
| stream_staging.oil_prices | New Brent observations |
| stream_staging.precipitation_moisture | New weather observations |
| cocoa_related.cocoa, brent_prices, precipitation | Existing curated history, initially seeded by [historical backfills](../../pipelines/backfill/README.md) |

For each feed, the procedure freezes the curated baseline and exact accepted
current acquisition. Equivalent deliveries collapse; conflicting keys fail.
Current source rows overlay matching baseline dates in isolated candidates.
The calendar runs from 2014-01-02 through the explicit release business date,
converts weather -999.0 to null and carries valid observations for at most
7 calendar days for cocoa, 14 for oil and 7 for each weather measurement.
The inclusive limit applies across weekends/holidays; age then expires to NULL.
Missing rows, explicit nulls and sentinels do not reset the original value date.
Source age/coverage still must pass the mandatory feed gates; further revision
precedence remains checklist item 17. All three source
tables and derived outputs publish together in the release transaction.

The output is cocoa_related.joined_daily_cocoa_oil_weather, with date,
cocoa_price, oil_price, precipitation, and soil_moisture.

## Why this precedes feature engineering

One row per calendar date makes the downstream window offsets meaningful:
a seven-row lag/window represents seven calendar days, and a 40-row lead
represents 40 calendar days. Forward fill repeats observations across gaps;
it does not create new source measurements. Values can remain null before
the first observation or beyond the carry limit.

The [bounded SQL fragment](bounded_join.sql) is embedded in the procedure.
`cocoa_related_studio.ls_series_quality` exposes per-measurement original dates,
ages, imputation/missingness, expiry and latest observation age as of today.
Its `usable_value` is NULL for expired/inconsistent values, including legacy
releases that still retain unlimited filled values. See the
[policy and acceptance](../../docs/operations/bounded-forward-fill.md).

**Next:** [2. Feature engineering](../02_feature_engineering/README.md).
See the [full sequence](../README.md).
