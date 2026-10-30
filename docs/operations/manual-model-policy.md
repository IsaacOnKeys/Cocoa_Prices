# Manual retraining and reviewed model selection

Checklist item 24. The owner selected manual retraining and review before
promotion on 2026-10-30. Daily releases now reuse the selected accepted model;
they contain no `CREATE MODEL` or automatic retraining path. Missing/deleted
models, incompatible features and invalid forecasts fail publication and
preserve the previous complete release under the existing failure policy.

## Accepted selection and reproducibility

Two small tables in the existing delivery dataset hold the singleton
`model_policy` selection and append-only `model_reviews`. The release registry
has an additive nullable `model_card` column. Each new run freezes model ID and
card with its inputs; retries use that frozen choice even if another model is
approved later. Approval changes new runs, not previously issued predictions.

The initial selection retains the model from the current accepted release.
Its original source release, eligible source rows, cutoff, metadata ETag and
feature contract are recorded. It is explicitly provisional: neither the local
simulation nor the original fitted scores prove its live predictive value.
Existing registry rows remain unchanged with NULL model cards where no card
was captured at their execution time.

## Manual candidate workflow

`scripts/operations/manage_models.py` previews by default, before importing
cloud clients. Nothing trains or changes selection without `--execute`.

1. Choose an accepted frozen source and an explicit historical training cutoff.
   Preview a uniquely named candidate:

   ```powershell
   .\.build\python311-ops\Scripts\python.exe scripts/operations/manage_models.py train --source-release <accepted-sha256> --business-date 2026-10-30 --cutoff 2026-05-01 --key candidate-example --output .build/candidate-example.json
   ```

2. Review the plan and remaining budget. Add `--execute` only when requesting
   training. This creates one immutable `cocoa_candidate_<sha256>` model and
   a receipt; it cannot change the serving selection. Identical requests reuse
   deterministic jobs. The tool never replaces an existing model in place.

3. Review the receipt and its source/feature assumptions. Promotion requires a
   reviewer, rationale, expected current review ID and explicit acknowledgement
   of revised-history limitations:

   ```powershell
   .\.build\python311-ops\Scripts\python.exe scripts/operations/manage_models.py promote .build/candidate-example.json --expected-review <current-review-sha256> --reviewer <operator> --reason '<review rationale>' --key review-example --acknowledge-revised-history
   ```

4. Only adding `--execute` performs the reviewed selection. It verifies the
   exact training job/source/cutoff, current model ETag/schema and held-out
   evidence before an atomic compare-and-swap of the selection plus review log.
   A stale expected review or reused decision ID with different contents fails.

The current selection/review ID is visible in
`cocoa_related_studio.model_health`. No candidate is promoted automatically,
including candidates that pass the numerical gate. Candidate receipts must be
kept with their review evidence; do not edit them to bypass a failed gate.

## Candidate and live-health gates

Engineering defaults for this small CV workload are:

- At least **365 eligible training rows**, with target date + one assumed cocoa
  publication day no later than the training cutoff. `NO_SPLIT` is used within
  that explicitly bounded training period.
- At least **20 matched held-out weekday origins** strictly after the cutoff,
  with mature observed target quotes and both naive baselines available.
  Eligibility is checked before starting paid training.
- No nonfinite/nonpositive held-out predictions. Candidate MAE must be no more
  than **5% worse than the better persistence/weekly-seasonal baseline**.
- Exactly the reviewed linear-regression type, nine predictors and 40-day
  label/output contract; the model version must match the receipt's ETag.

These defaults are project choices, not industry accuracy standards. Candidate
held-out evaluation uses the actual candidate BigQuery model, but historical
source features are revised vintages. Its status remains
`provisional_revised_history_holdout`; it is not claimed as verified past
publisher availability. Live issued-forecast evaluation remains separate.

`model_health` shows the selected next-run model and the currently serving
release/model separately. Until 20 genuine matched outcomes exist, performance
is provisional. Thereafter a model MAE more than 5% worse than the better naive
baseline produces `review_required_baseline_degradation`. This requests operator
review; it does not silently retrain, switch models or rewrite issued history.
Forecast validity gates remain mandatory regardless of sample size. Alert
delivery is implemented separately under item 26.

## Recovery

Select a retained prior approved model with the same explicit review controls:

```powershell
.\.build\python311-ops\Scripts\python.exe scripts/operations/manage_models.py restore <prior-review-sha256> --expected-review <current-review-sha256> --reviewer <operator> --reason '<incident reason>' --key recovery-example
```

Preview first; `--execute` restores the retained choice after metadata/version
verification. It affects new releases. If the already published data/forecast
release itself must be restored, use the existing guarded `rollback_release.py`
workflow as well. Prior approved models and issued outputs remain retained.

## Deployment and cost

Apply `model_policy_migration`, seed the current accepted model with its real
metadata, and install `model_selection`, the updated daily procedure and
`model_health_view` before installing the DAG's verified procedure pin.
Back up the prior DAG/procedure. No new service, dataset, IAM role or dependency
is required. Existing runtimes remain Python 3.11.

Manual jobs have deterministic identities, a ten-minute job timeout and a
128 MiB maximum-bytes-billed setting. These are workload bounds, not a monthly
spend cap. Model creation has different pricing from normal prediction/query
jobs; inspect the applicable training pricing and current account spend before
manual execution. [Google's pricing](https://cloud.google.com/bigquery/pricing)
and [ML dry-run guidance](https://docs.cloud.google.com/bigquery/docs/bqml-introduction)
describe this distinction. The budget target remains under EUR 5/month, maximum
EUR 10/month including validation; current billing totals are not verified.

Completed 2026-10-30 on `codex/manual-model-promotion`. Eighteen focused Python
3.11 checks and changed-file Ruff/whitespace validation passed. They cover
preview-only behavior, label availability, insufficient samples, invalid and
degraded candidates, metadata version/schema checks, frozen model reuse,
stale selection and provisional/live health statuses. Existing full pipeline
and earlier-feature test suites were not rerun.

Native validation retained the currently accepted
`models.cocoa_release_5824ad9c522a68f129ae45e558be74b687f1319ff94f6c282bab26e47c990b8c`
model, with 4,645 eligible source rows. A stale-review call aborted without
changing selection. The reviewed inference path produced one finite positive
December 9 forecast. A manual candidate statement passed a dry run; **no new
model was trained or promoted during feature validation**. The existing model
remains explicitly provisional while genuine outcomes accumulate.

The installed daily procedure SHA256 is
`4b93150cabe66ca20d017b5449d27084887c391b58357e3180d7a2415cbefe45`;
the DAG SHA256 is
`80a822d9bd253a620abdbb6c243028e76bba78f5fbb3a5d55bd7556c5be679a8`.
Native Airflow import verified the pin. Installation used the VM's existing
scheduled running window and preserved its running state. The previous DAG is
`/opt/airflow/releases/manual-model-policy-20261030/cocoa_daily_dag.py.before`;
the prior procedure and native receipts are retained in
`.build/manual-model-policy/`. Candidate training/promotion with a newly trained
model remains a manually requested workflow, not a claimed cloud training test.
