# Feature engineering

The complete feature definitions now live in the SQL workflow's dedicated
[02_feature_engineering/](../../sql/02_feature_engineering/README.md) directory.
It documents current price, 1/7/28-day cocoa lags, seven-row oil/weather averages,
weekday/month categories, the 40-day target, and missing/forward-filled values.

Follow the full sequence:

[Curated data](../../sql/01_curated_data/README.md) ->
[feature engineering](../../sql/02_feature_engineering/README.md) ->
[model training](../../sql/03_model_training/README.md) ->
[prediction](../../sql/04_prediction/README.md) ->
[backtesting / evaluation](../../sql/05_evaluation/README.md).

The expressions remain in the single
[canonical procedure](../../sql/procedures/run_daily_pipeline.sql#L97-L120).
See the [SQL index](../../sql/README.md) for the complete inputs/outputs map.
