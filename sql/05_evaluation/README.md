# 5. Backtesting / evaluation

This directory makes the evaluation stage visible while distinguishing what
the current SQL implements from a rigorous chronological backtest.

**Implementation:** [candidate historical predictions and actual alignment](../procedures/run_analytics_release.sql);
the existing mape_40d view reads the atomically accepted prediction table.

## Current implementation

Inputs are full-history predictions from the release-specific model and actual
cocoa prices from that release's frozen candidate features.

| Output | Contents |
| --- | --- |
| cocoa_related.pred_hist_40d | prediction_date, forecast_date, yhat_40, and actual price joined on forecast_date |
| cocoa_related.mape_40d | AVG(SAFE_DIVIDE(ABS(actual - yhat_40), actual)) * 100, restricted to non-null actuals |

Actuals become available only when the corresponding forecast date exists in
the observed feature history. Missing actuals are excluded from MAPE.
Zero-denominator contributions follow existing SAFE_DIVIDE/AVG semantics.

## What the current result establishes

It measures errors in the existing historical-prediction table. The model was
fitted once using the current labeled history, then reused to score that
history. AUTO_SPLIT does not supply an explicit time-ordered evaluation design.

Consequently, this MAPE is not established chronological out-of-sample forecast
performance. It should not be presented as a validated rolling-origin backtest.

## Chronological evaluation

The [local chronological simulation](../../scripts/operations/backtest_forecasts.py)
refits at explicit time cutoffs, accounts for the 40-day label delay and assumed
source-publication lags, and compares against persistence and weekly seasonal
baselines. It is labeled revised-history simulation because older publisher
vintages are incomplete; its results are not deployed-model performance.

The [issued-forecast evaluation views](../serving/issued_forecast_evaluation.sql)
score genuinely issued single-point forecasts as observed actuals arrive,
preserving actual-vintage identity and explicit pending/coverage statuses.
Read [the evaluation policy and reproduction steps](../../docs/operations/chronological-evaluation.md)
before interpreting either result. `mape_40d` retains its historical fitted-score
meaning and is not an out-of-sample metric.

**Previous:** [4. Prediction](../04_prediction/README.md).
See the [full sequence](../README.md).
