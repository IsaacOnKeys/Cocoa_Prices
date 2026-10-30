# Chronological evaluation and its limits

Checklist item 23, branch `codex/chronological-forecast-evaluation`. The owner
selected a labeled historical simulation plus genuinely issued-forecast
evaluation on 2026-10-30. Neither path trains cloud models or changes production
forecasts. Historical fitted-model `mape_40d` remains retrospective scoring;
it is not substituted for either evaluation below.

## Local revised-history simulation

`scripts/operations/export_evaluation_inputs.py` exports one accepted raw
source snapshot, with release/cutoff/code/model metadata and a CSV SHA256.
It reads only retained artifacts; each query is capped at 50 MiB. Subsequent
backtests read the local CSV and require no Google credentials or cloud jobs.

`scripts/operations/backtest_forecasts.py` builds time-ordered features and
refits a local ridge model at each requested origin. It uses the seven numeric
and two calendar predictors, training-only median imputation, standardization,
one-hot calendar encoding and fixed L2 alpha 1. There is no tuning on held-out
outcomes. This is a local candidate from the same model family, not a bitwise
reproduction or an accuracy claim for the deployed BigQuery ML model.

Every training example must have an observed target on origin +40 days, with
that target's assumed publication date no later than the training cutoff.
Prediction uses only values exposed by the configured source lags and the
existing 7/14/7/7-day carry limits. Missing observations and NASA -999 sentinels
do not refresh observation age. All preprocessing is fitted anew on the
eligible training rows, never on the prediction row or later outcomes.

Default publication-lag assumptions are **1 calendar day for cocoa and 3 for
oil, rain and soil**. These are configurable simulation assumptions, not
recovered historical publisher release dates. The input uses the accepted
snapshot's revised history. Missing older vintages mean the result **does not
prove historical point-in-time source availability**. The report records this
limitation, input/code hashes, source snapshot, dependencies, period, lags,
training sizes and latest training target at every origin.

Model, persistence and weekly seasonal-naive errors use identical scored
origins. Persistence uses the last known price at origin. Weekly seasonal
naive uses the most recent target-weekday price that could be known under the
assumed lag. Origins without an actual target-date quote, an available baseline
or enough training data are listed explicitly. Weekend origins are skipped.
MAE/RMSE are EUR per metric tonne; MAPE is percent. Empty samples return NULL
metrics. Finite nonpositive model predictions remain in error metrics and flag
the candidate invalid, rather than being removed to improve apparent accuracy.

Run locally in a separate Python 3.11 environment:

```powershell
py -3.11 -m venv .build/python311-evaluation
.\.build\python311-evaluation\Scripts\python.exe -m pip install -r requirements-evaluation.txt
.\.build\python311-ops\Scripts\python.exe scripts/operations/export_evaluation_inputs.py --release <accepted-sha256> --business-date 2026-10-30 --output .build/forecast-evaluation/observations.csv
.\.build\python311-evaluation\Scripts\python.exe scripts/operations/backtest_forecasts.py .build/forecast-evaluation/observations.csv --as-of 2026-10-30 --start 2025-09-19 --end 2026-09-18 --step-days 7 --output .build/forecast-evaluation/simulation.json
```

The output path must be new. Both raw inputs and reports remain outside Git
under `.build`; retain the CSV metadata/report together when sharing recovery
evidence. The retained source release permits reproducible export. These local
dependencies are not added to Airflow, Beam or Cloud Run runtimes.

## Genuinely issued forecasts

Render `issued_evaluation_views` using the existing SQL renderer:

- `issued_forecast_evaluation_vintages` joins original accepted
  `single_40_day_ahead_v1` forecasts to observed target quotes in each retained
  accepted actual-data vintage. Issue/model/cutoff and actual-vintage identities
  remain explicit; a revised actual does not erase the earlier vintage.
- `issued_forecast_evaluation` selects the currently active actual vintage.
- `issued_forecast_metrics` reports paired model, persistence and weekly
  seasonal errors with sample counts and the scored-origin period. The baselines
  use the original issue's frozen prices, not today's revised origin history.

This evaluates rolling real forecast origins. Legacy forty-origin curves and
fitted historical rescores are excluded. An issue or input cutoff on/after the
target day is excluded from accuracy. A rollback to an older input snapshot,
an immature target, an unquoted target date or an unavailable baseline has an
explicit status. Only exact observed target quotes are scored: weekends and
market closures without a quote remain unscored, rather than using a filled
price as an observed actual. Sample counts disclose this coverage limitation.

All three methods share the same eligible sample. With no issues or no mature
outcomes, metrics are NULL and the status is `no_issued_forecasts` or
`insufficient_matured_forecasts`. A forty-day target needs time to mature;
absence of those results must not be replaced with simulated accuracy.

These are views over retained data, with no additional scheduler, storage copy,
VM or cloud training. History exploration should filter origin/as-of dates.
The budget remains target under EUR 5/month, maximum EUR 10/month including
validation; actual billing totals have not been verified.

## Validation and initial result

Completed 2026-10-30. Twenty-two focused Python 3.11 tests passed, including
future-data perturbation, 40-day label availability, source lag/expiry,
training refits, invalid predictions, matched baseline samples, actual-vintage
revisions, early/late issues and zero-sample reporting. Changed-file Ruff,
whitespace validation and the isolated evaluation environment's `pip check`
passed. Existing feature/cloud pipeline suites were not rerun.

One accepted raw snapshot exported 4,593 rows, billing 40 MiB. The local run
refitted 53 models at weekly origins from 2025-09-19 through 2026-09-18, with
the default publication-lag assumptions and at least 365 training observations:

| Local simulated method | Scored origins | MAE (EUR/tonne) | RMSE (EUR/tonne) | MAPE (%) |
| --- | ---: | ---: | ---: | ---: |
| Ridge | 53 | 681.32 | 902.14 | 17.68 |
| Persistence | 53 | 672.68 | 872.37 | 17.65 |
| Weekly seasonal naive | 53 | 668.59 | 864.14 | 17.52 |

The local ridge candidate **did not beat either baseline**. This is a useful
negative result, not evidence of deployed-model accuracy or justification for
automatic promotion. Model promotion/cadence is item 24.

All three issued-evaluation views were created at 16:28:57-16:28:58 UTC. One
native read billed 50 MiB and confirmed all three methods return
`no_issued_forecasts`, zero scored samples and NULL errors before the first
new-contract issue. No historical rescoring was substituted. Total feature
validation billed 90 MiB of reads; no cloud training, VM startup or pipeline
execution was performed.

Evidence: `.build/forecast-evaluation/observations.metadata.json`,
`simulation-with-provenance.json` and `deployed.json`. The report embeds its
input/code hashes, source-release metadata and numerical dependency versions.
Aggregate results are recorded here; raw licensed source observations remain
outside Git.
