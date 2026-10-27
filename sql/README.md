# Curated data to evaluation

The folder structure makes the conceptual sequence explicit:

~~~text
sql/
  01_curated_data/          MERGE, calendar alignment, forward fill
  02_feature_engineering/   price lags, oil/weather windows, calendar, target
  03_model_training/        BigQuery ML linear regression
  04_prediction/            40-day forecasts and historical scoring
  05_evaluation/            actual-price alignment, MAPE, backtest limitations
  procedures/
    run_daily_pipeline.sql
~~~

[Curated data](01_curated_data/README.md) ->
[feature engineering](02_feature_engineering/README.md) ->
[model training](03_model_training/README.md) ->
[prediction](04_prediction/README.md) ->
[backtesting / evaluation](05_evaluation/README.md).

Each numbered directory explains its stage's inputs, outputs and decisions,
with direct links to the implementing statements. These directories provide
the conceptual review order. Execution remains in the single canonical
[run_daily_pipeline.sql](procedures/run_daily_pipeline.sql) procedure.

| Stage | Input | Output | Implementation |
| --- | --- | --- | --- |
| [1. Curated data](01_curated_data/README.md) | Historical base tables and daily staging | Curated base tables; joined_daily_cocoa_oil_weather | [SQL stages 1-2](procedures/run_daily_pipeline.sql#L4-L87) |
| [2. Feature engineering](02_feature_engineering/README.md) | Aligned daily prices and weather | features_live: current price, 1/7/28-day lags, inclusive seven-row oil/weather means, weekday/month, target_40 | [SQL stage 3](procedures/run_daily_pipeline.sql#L90-L113) |
| [3. Model training](03_model_training/README.md) | Engineered predictors and observed target_40 labels | models.cocoa_linreg_40 | [SQL stage 4](procedures/run_daily_pipeline.sql#L115-L128) |
| [4. Prediction](04_prediction/README.md) | Model and recent/full-history feature rows | forecast_40d and historical scores | [SQL stages 5-6](procedures/run_daily_pipeline.sql#L130-L157) |
| [5. Backtesting / evaluation](05_evaluation/README.md) | Historical scores and observed prices | pred_hist_40d; mape_40d | [SQL stages 6-7](procedures/run_daily_pipeline.sql#L145-L172) |

The horizon is 40 calendar days because the joined input has one row per
calendar date. Historical scoring uses a separate ML.PREDICT call across the
full feature history; evaluation does not consume the recent forecast_40d table.

The current historical predictions and MAPE reuse the fitted model and are
not a rigorous chronological out-of-sample backtest. The
[evaluation stage](05_evaluation/README.md) explains this boundary explicitly.

## Runtime and source reconciliation

Airflow calls the deployed
cocoa-prices-430315.cocoa_related.run_daily_pipeline procedure by FQN.
It does not read this source filename or the numbered directories.

The initial directory migration preserved the original repository SQL with
SHA-256:

~~~text
bf8d85aa80ff4f58fa08516fc83a35fe454d9e00841ba75f382205959502a43a
~~~

Live release validation on 2026-10-27 found that the deployed procedure already
used a calendar from 2014-01-02 through CURRENT_DATE() and converted weather
sentinels (-999.0) to null before forward fill. The repository source was
reconciled with that existing deployed definition; executable SQL tokens match
after comments, whitespace and the final delimiter are normalized.

No SQL fragments, renderer, duplicate maintained definitions, model changes
or BigQuery procedure deployment were introduced. Airflow continues to execute
the existing deployed procedure.
