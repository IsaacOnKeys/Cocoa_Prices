# 2. Feature engineering

This stage turns the aligned daily cocoa, oil, and weather series into the
model's inputs and its 40-day-ahead supervised target.

**Implementation:** [feature expressions in the canonical procedure](../procedures/run_daily_pipeline.sql#L90-L113).

**Input:** cocoa_related.joined_daily_cocoa_oil_weather.
**Output:** cocoa_related.features_live, a BigQuery view.

## Feature definitions

| Group | Output | Existing expression / meaning |
| --- | --- | --- |
| Current price | y | Current forward-filled cocoa_price; included as a predictor. |
| Price history | y_lag1 | LAG(y, 1): the previous calendar day's cocoa price. |
| Price history | y_lag7 | LAG(y, 7): cocoa price seven calendar days earlier. |
| Price history | y_lag28 | LAG(y, 28): cocoa price 28 calendar days earlier. |
| Oil context | oil_ma7 | AVG(oil_price) over the current row and six preceding rows. |
| Weather context | precip_ma7 | The same inclusive seven-row average for precipitation. |
| Weather context | soil_ma7 | The same inclusive seven-row average for soil moisture. |
| Calendar | dow | EXTRACT(DAYOFWEEK FROM date), cast to string. |
| Calendar | mon | EXTRACT(MONTH FROM date), cast to string. |
| Supervised label | target_40 | LEAD(y, 40): cocoa price 40 calendar days later. |
| Audit/join key | date | Retained for alignment and assigning forecast dates; excluded from model training. |

All windows are ordered by date. The nine predictors combine current/recent
price context, smoothed exogenous conditions, and calendar categories.
The target is a label, not an input feature.

## Window and target construction

The [curated stage](../01_curated_data/README.md) supplies a complete daily
calendar. Lag/lead offsets therefore refer to calendar days rather than only
trading days. Rolling windows include the current day; early windows use the
available non-null rows, and early lag values may be null.

Forward fill can repeat values across weekends and missing observations.
These are characteristics of the existing feature construction that matter
when interpreting the model.

The final 40 daily rows have no observed target_40 and are excluded from
training. They remain available for prediction. Training selects all view
columns except date, with target_40 designated as the label.

## Evaluation boundary

The presence of lagged/exogenous features does not establish that they improve
forecast accuracy. The current model uses AUTO_SPLIT and historical predictions
from the fitted model, rather than an explicit chronological holdout.
Chronological evaluation and feature-set comparisons remain
[engineering work](../../docs/operations/engineering-backlog.md).

**Previous:** [1. Curated data](../01_curated_data/README.md).
**Next:** [3. Model training](../03_model_training/README.md).
See the [full sequence](../README.md).
