# 5. Backtesting / evaluation

This directory makes the evaluation stage visible while distinguishing what
the current SQL implements from a rigorous chronological backtest.

**Implementation:** [historical predictions and actual-price join](../procedures/run_daily_pipeline.sql#L145-L165)
and [MAPE view](../procedures/run_daily_pipeline.sql#L167-L172).

## Current implementation

Inputs are full-history predictions from models.cocoa_linreg_40 and actual
cocoa prices from cocoa_related.features_live.

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

## Work needed for chronological backtesting

- Define time-ordered training cutoffs and evaluation periods.
- At each cutoff, train only on examples whose 40-day targets are already known.
- Score future observations without fitting or selecting features on them.
- Aggregate errors across forecast origins and compare against a baseline.

These are [future engineering tasks](../../docs/operations/engineering-backlog.md),
not changes made by this folder organization.

**Previous:** [4. Prediction](../04_prediction/README.md).
See the [full sequence](../README.md).
