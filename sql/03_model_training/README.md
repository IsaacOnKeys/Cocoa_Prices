# 3. Model training

This stage fits the model that maps the engineered inputs at date t to the
cocoa price at date t + 40 calendar days.

**Implementation:** [training guard and CREATE MODEL](../procedures/run_daily_pipeline.sql#L122-L135).

| Contract | Existing behavior |
| --- | --- |
| Input | cocoa_related.features_live |
| Training rows | target_40 IS NOT NULL |
| Predictors | The nine [engineered predictors](../02_feature_engineering/README.md); date is excluded |
| Label | target_40 |
| Output model | models.cocoa_linreg_40 |
| Model type | BigQuery ML linear_reg |
| Regularization | l2_reg = 1.0 |
| Split option | data_split_method = AUTO_SPLIT |

The procedure recreates the model only if at least one non-null target exists.
Forecasting and historical evaluation are inside the same guard. If no labels
exist, these statements are skipped; this is not a claim that older output
objects are cleared.

The current model is a fixed linear-regression baseline with L2 regularization.
No tuning or feature-ablation results are established by this implementation.
AUTO_SPLIT is not an explicitly chronological holdout, so its presence does not
prove out-of-sample forecasting performance.

**Previous:** [2. Feature engineering](../02_feature_engineering/README.md).
**Next:** [4. Prediction](../04_prediction/README.md).
See the [full sequence](../README.md).
