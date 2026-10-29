# 3. Model training

This stage fits the model that maps the engineered inputs at date t to the
cocoa price at date t + 40 calendar days.

**Implementation:** [training guard and CREATE MODEL](../procedures/run_analytics_release.sql).

| Contract | Existing behavior |
| --- | --- |
| Input | Frozen candidate features |
| Training rows | target_40 IS NOT NULL |
| Predictors | The nine [engineered predictors](../02_feature_engineering/README.md); date is excluded |
| Label | target_40 |
| Output model | models.cocoa_release_<release_id>; active identity in cocoa_related_studio.release_status |
| Model type | BigQuery ML linear_reg |
| Regularization | l2_reg = 1.0 |
| Split option | data_split_method = AUTO_SPLIT |

The procedure requires non-null training labels and creates a new versioned
model before serving publication. A retry reuses that model and its frozen
inputs. Missing labels or invalid forecast candidates fail the complete release,
preserving the accepted data/model identity. The old cocoa_linreg_40 model
remains the legacy baseline; minimum sample/evaluation/promotion policy is
separate checklist work.

The current model is a fixed linear-regression baseline with L2 regularization.
No tuning or feature-ablation results are established by this implementation.
AUTO_SPLIT is not an explicitly chronological holdout, so its presence does not
prove out-of-sample forecasting performance.

**Previous:** [2. Feature engineering](../02_feature_engineering/README.md).
**Next:** [4. Prediction](../04_prediction/README.md).
See the [full sequence](../README.md).
