# 3. Model training

This stage fits the model that maps the engineered inputs at date t to the
cocoa price at date t + 40 calendar days.

**Implementation:** [manual candidate tool](../../scripts/operations/manage_models.py)
and [daily reviewed-model reuse](../procedures/run_analytics_release.sql).

| Contract | Existing behavior |
| --- | --- |
| Input | Frozen candidate features |
| Training rows | Manual candidate only: non-null target_40 with target + one publication day <= explicit training cutoff; minimum 365 rows |
| Predictors | The nine [engineered predictors](../02_feature_engineering/README.md); date is excluded |
| Label | target_40 |
| Output model | Immutable manual candidate; serving choice is reviewed and frozen into each release |
| Model type | BigQuery ML linear_reg |
| Regularization | l2_reg = 1.0 |
| Split option | NO_SPLIT within a chronological training period, followed by a separate held-out interval |

Daily execution performs inference with the reviewed model and never trains.
A retry retains that model choice and its frozen inputs. Invalid forecasts
fail the complete release, preserving the accepted data/model identity.
Old models and issued outputs remain retained for recovery. See
[manual review, gates, health and recovery](../../docs/operations/manual-model-policy.md).

The current model is a fixed linear-regression baseline with L2 regularization.
No tuning or feature-ablation results are established by this implementation.
Earlier accepted models trained with AUTO_SPLIT remain explicitly provisional;
their fitted historical scores do not prove out-of-sample performance. Candidate
holdouts use revised history, and genuine issued-forecast errors are tracked
separately as targets mature.

**Previous:** [2. Feature engineering](../02_feature_engineering/README.md).
**Next:** [4. Prediction](../04_prediction/README.md).
See the [full sequence](../README.md).
