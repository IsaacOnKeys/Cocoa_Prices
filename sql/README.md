# Analytics release SQL

The canonical release procedure is
[run_analytics_release.sql](procedures/run_analytics_release.sql). Render validated
identifiers with [render_delivery_sql.py](../scripts/deploy/render_delivery_sql.py).
Airflow calls `analytics_delivery.run_daily_pipeline` with a logical release,
business date, exact accepted acquisitions and a verified procedure-body hash.

The procedure freezes its input snapshot, builds candidates and a versioned
model, retains artifacts, then commits all six serving tables and the active
release marker in one DML transaction. Permanent model/table creation stays
outside that transaction. The old [zero-argument entrypoint](procedures/run_daily_pipeline.sql)
explicitly refuses direct publication. Its previous definition remains in Git
history and the cutover recovery archive.

The renderer embeds [input](quality/input_checks.sql) and
[output](quality/output_checks.sql) quality fragments in that procedure. Their
39 named checks retain actionable error/warning results before publication;
the fragments are not independent deployment scripts. See
[recurring gate acceptance](../docs/operations/recurring-quality-gates.md).
The [bounded carry fragment](01_curated_data/bounded_join.sql) limits calendar
fill to 7/14/7/7 days. The [measurement quality view](serving/imputation.sql)
exposes original dates, age, missingness, imputation and expiry from retained
accepted artifacts, including after rollback. See
[fill-policy acceptance](../docs/operations/bounded-forward-fill.md).
The [revision selector](01_curated_data/select_revisions.sql) preserves known
facts when a current measurement is missing, retains the incoming revision and
exposes preservation flags/identities in the quality view. See
[revision-policy acceptance](../docs/operations/deterministic-source-revisions.md).
Publication returns a cached receipt after commit; the Airflow/full diagnostic
query budget is bounded at 2 GiB, including first-call model creation.

| Stage | Implementation / behavior |
| --- | --- |
| [1. Curated data](01_curated_data/README.md) | Frozen curated baseline plus the accepted current acquisition; candidate source series and daily join |
| [2. Features](02_feature_engineering/README.md) | Current price, 1/7/28-day lags, seven-row means, calendar and 40-day label |
| [3. Model](03_model_training/README.md) | A retained `models.cocoa_release_<release_id>` trained from frozen candidate features |
| [4. Prediction](04_prediction/README.md) | Forty unique future dates and historical scores from that model |
| [5. Evaluation](05_evaluation/README.md) | Existing fitted-model historical error; chronological evaluation remains separate work |

[020_atomic_delivery.sql](migrations/020_atomic_delivery.sql) creates permanent
release-control/artifact tables. The [explicit bootstrap](migrations/020_atomic_delivery_bootstrap.sql)
seeds an independently verified baseline without inventing publication times.
[Rollback](procedures/rollback_analytics_release.sql) verifies the current marker
and retained fingerprints before restoring all six tables atomically.
[Serving views](serving/analytics_delivery.sql) bind freshness and model/status
metadata to the accepted release, including the corrected EIA supplier label.

The stable `features_live` and `mape_40d` views continue to read accepted serving
tables; candidate training never reads a partially updated live view. Read
`cocoa_related_studio.release_status` for the active release/model identity.
The legacy `models.cocoa_linreg_40` remains preserved for the verified baseline.

See [atomic publication and validation](../docs/operations/atomic-analytics-release.md)
for deployment, recovery, native failure injection and remaining quality/model
boundaries. The dated [October 27 receipt](../docs/operations/release-validation-2026-10-27.md)
describes the earlier procedure and remains historical evidence.
