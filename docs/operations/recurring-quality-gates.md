# Recurring candidate quality gates

Checklist item 15, branch `codex/recurring-quality-gates`, implementation
`cf00515`, deployed pin `e56702f`. The quality procedure and nine-task Airflow DAG
are installed. The accepted serving release remains the verified October 28
baseline documented in [item 20](atomic-analytics-release.md).

The feed gates pass their exact generation/hash-verified observation manifests
to release SQL, preserving revision keys, real source calendars and explicit
missingness. This adds evidence to the existing count, window and age fields;
there are no new producer, Avro, consumer, Beam or dependency requirements.

The canonical input and output checks live in
[input_checks.sql](../../sql/quality/input_checks.sql) and
[output_checks.sql](../../sql/quality/output_checks.sql). The deployment renderer
inserts those exact fragments into the atomic procedure. The native diagnostic
executes the same fragments against independent bounded fixtures.

Input errors cover source-window shape/bounds, expected revision sets,
duplicate keys, missingness agreement, valid dates/provenance, finite values and
documented domains. Legacy weather -999 is recognized as missing baseline data;
current source -999 cannot become a valid value. Nullable measurements retain
their explicit source meaning. Finite oil prices have no new sign restriction.
Source freshness is calculated from the actual current acquired measurements:
cocoa 7 days, oil 14, and each weather measurement 7. A complete filled calendar
cannot satisfy a failed source-age check.

Output errors reconcile joined values to frozen source revisions and the
declared fill rule, feature date keys, price lags/labels, seven-row averages,
calendar categories, finite values and physical domains. They also check exactly
40 unique positive finite forecasts in the correct target calendar, complete
historical scoring, 40-day horizon/actual alignment and finite historical MAPE.
Expected leading/warm-up/terminal label nulls remain permitted. The historical
metric is still fitted-model rescoring; these gates make no predictive-value or
chronological evaluation claim.

Gross 100x price-revision and forecast scale changes are recorded as warnings.
They are screening signals, not established market-validity or model-performance
limits. Hard source/domain/structural errors withhold the complete release.
Model promotion/performance policy remains item 24; bounded carry-forward and
revision precedence remain items 16-17.

Every executed check records stage, name, severity, violation count and a short
operator explanation in `analytics_delivery.release_runs.quality_results`.
The report persists before an ASSERT can stop the build; the exception handler
marks the run failed while preserving that evidence. Accepted releases retain
their check array and warning count alongside the artifact fingerprints exposed
by `cocoa_related_studio.release_status`. No serving table or active marker is
changed before mandatory checks pass.

## Validation

`scripts/diagnostics/validate_quality.py` uses a labelled disposable dataset and
independent closed-form fixtures to exercise valid data and deliberate source,
feature, forecast and scoring faults. It also checks legitimate nulls, legacy
sentinels, finite negative oil and warning severity. The full atomic diagnostic
uses complete accepted observation manifests and runs the integrated procedure,
failure injection and frozen retries. Exact native/production outcomes and held
evidence are retained with the completion manifest described below. No current-day
successful production publication is claimed while the NASA age breach continues.

The final checker contains 38 named checks. Native quality fixture
`20261029_2697ac8f` verified 28 cases, including an additional equal-count but wrong
historical-origin regression. Integrated fixture `20261029_204e5783` verified
publication, all three failure points, frozen/completed retries, six-table
rollback, stale candidate/current-marker guards and altered-artifact rejection.
A fresh final build `20261029_f7d97031` passed the complete 38-check procedure,
actual model training, publication and completed retry. The three injected
failures and recovery tests ran on the preceding integrated build; the additional
historical-origin guard was separately tested with valid and invalid data.
These are actual BigQuery tests with labelled synthetic data, not supplier or
rendered-report acceptance. The gross-scale warning fixture initially lacked
BigQuery's required WHERE clause; after confirming it inserted no case row,
the corrected fixture was rerun and its warning severity verified.

## Production acceptance, October 29

Controlled run `quality_gates__cf00515__20261029` ran from
22:15:19.876866 to 22:15:53.964880 UTC. Cocoa and oil passed with complete
22- and 21-observation manifests. NASA precipitation age 8 exceeded the unchanged
7-day limit; SQL and summary were upstream-failed. All six serving fingerprints,
the accepted marker and legacy model stayed unchanged. Four BI source views
remained readable. This is actual withholding evidence, not a successful current
production publication or rendered BI acceptance.

Installed DAG SHA256:
`28cd11fb450c17bfd1e3d4549d0133e20d8f6b6844d0f277ea52011af67e1f78`.
Its native-verified procedure-body SHA256:
`232ec5bd2fa5dae412b22dab646e480e4cb9201bfd47d30ac411d2cb8ad55fc5`.
The BigQuery task and RPC adapter are unchanged apart from the procedure pin;
their prior native Python 3.11/Airflow/SDK acceptance remains in item 20.
The final checkpoint is unpaused/idle, legacy daily Beam remains disabled, and
the maintenance VM is restored to its prior stopped state. All seven labelled
synthetic datasets/models were removed; protected production snapshots remain.

All 497 credential-free tests, Ruff, 72 Python and five Bash syntax checks passed.
Both Terraform roots have no drift and retain 122 managed resources. No image,
package, encryption, producer, consumer or runtime-grant changes were needed.
Held exact changed source, previous DAG/procedure and native/production receipts
are under `gs://raw-historic-data/operations/2026-10-29/quality-gates/`.
The ignored parent checklist records the completion manifest generation/SHA256.
This does not establish overall service-level attainment, forecast accuracy,
bounded imputation or chronological model evaluation.
