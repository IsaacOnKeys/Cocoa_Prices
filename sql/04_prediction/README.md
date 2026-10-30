# 4. Prediction

The trained model is scored with ML.PREDICT. Each input row at date t produces
a prediction for date t + 40 calendar days.

**Implementation:** [candidate forecasts and historical scoring](../procedures/run_analytics_release.sql).

## Forecast output

Inputs are the release-specific model and the one frozen candidate feature
row at the release business date. The input selection removes target_40. The model identity is
retained in analytics_delivery.release_runs and the active release_status view.

The output table cocoa_related.forecast_40d contains:

| Column | Meaning |
| --- | --- |
| forecast_date | Input date + 40 days |
| forecast_cocoa_price | predicted_target_40 from the fitted model |

The owner-selected product is exactly one forecast for business date +40
calendar days. Quality gates reject zero/multiple rows or any other target.
Use `cocoa_related_studio.current_forecast` for explicit origin, issue/cutoff,
target, model, horizon and unit labels. Actual observation ages and bounded
carry remain visible through the existing measurement-quality view.
See [the product definition](../../docs/operations/forecast-product.md).

Older retained releases contain predictions from the previous last-40-origins
selection; their issue snapshots remain unchanged. They are not a model trained
for horizons 1-40. The new serving view selects only their business-date +40
point and labels their contract version as legacy until a new release publishes.

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
