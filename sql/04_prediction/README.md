# 4. Prediction

The trained model is scored with ML.PREDICT. Each input row at date t produces
a prediction for date t + 40 calendar days.

**Implementation:** [recent-row forecasts](../procedures/run_daily_pipeline.sql#L130-L143)
and [full-history scoring for evaluation](../procedures/run_daily_pipeline.sql#L145-L157).

## Forecast output

Inputs are models.cocoa_linreg_40 and the most recent 40 calendar rows from
cocoa_related.features_live. The input selection removes target_40.

The output table cocoa_related.forecast_40d contains:

| Column | Meaning |
| --- | --- |
| forecast_date | Input date + 40 days |
| forecast_cocoa_price | predicted_target_40 from the fitted model |

With a complete daily calendar, those recent rows provide forecast dates from
one through 40 days beyond the latest feature date. Each remains a direct
40-day-ahead prediction from its own input row; the procedure does not
recursively feed forecasts back into the features.

## Historical scoring

The evaluation branch also calls ML.PREDICT, this time across the full feature
history. It passes prediction_date, forecast_date, and yhat_40 into the
[actual-price alignment and evaluation stage](../05_evaluation/README.md).
That branch does not read forecast_40d.

The conceptual review order is prediction followed by evaluation; recent
forecast output and full-history scoring are separate uses of the same model.

**Previous:** [3. Model training](../03_model_training/README.md).
**Next:** [5. Backtesting / evaluation](../05_evaluation/README.md).
See the [full sequence](../README.md).
