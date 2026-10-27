# 1. Curated data

The analytical workflow starts by aligning the historical and daily source
records into one daily series.

**Implementation:** [staging deduplication and MERGE](../procedures/run_daily_pipeline.sql#L4-L43)
and [daily join and forward fill](../procedures/run_daily_pipeline.sql#L45-L87).

## Inputs and outputs

| Input | Role |
| --- | --- |
| stream_staging.cocoa_prices | New cocoa observations |
| stream_staging.oil_prices | New Brent observations |
| stream_staging.precipitation_moisture | New weather observations |
| cocoa_related.cocoa, brent_prices, precipitation | Existing curated history, initially seeded by [historical backfills](../../pipelines/backfill/README.md) |

For each feed, the procedure chooses the latest ingestion per date before
MERGE updates/inserts its curated base table. It then generates a calendar from
2014-01-02 through CURRENT_DATE() (UTC), joins the three sources, converts
weather sentinel values of -999.0 to null, and forward-fills each non-null
series through the current row. This matches the deployed procedure captured
during release validation on 2026-10-27; calendar extension can repeat the latest
available observations when a source has not published a new measurement.

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
