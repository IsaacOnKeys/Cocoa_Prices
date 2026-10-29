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
converts weather -999.0 to null and forward-fills non-null observations.
Source age/coverage must pass the mandatory feed gates; bounded imputation and
further revision precedence remain checklist items 16-17. All three source
tables and derived outputs publish together in the release transaction.

The output is cocoa_related.joined_daily_cocoa_oil_weather, with date,
cocoa_price, oil_price, precipitation, and soil_moisture.

## Why this precedes feature engineering

One row per calendar date makes the downstream window offsets meaningful:
a seven-row lag/window represents seven calendar days, and a 40-row lead
represents 40 calendar days. Forward fill repeats observations across gaps;
it does not create new source measurements. Values can remain null before
the first observation.

**Next:** [2. Feature engineering](../02_feature_engineering/README.md).
See the [full sequence](../README.md).
