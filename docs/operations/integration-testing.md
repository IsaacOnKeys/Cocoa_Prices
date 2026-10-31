# Isolated integration tests and runtime dependencies

Ordinary CI remains credential-free and Python 3.11 only. The manually dispatched `Runtime components` workflow adds real Airflow/DirectRunner execution and five isolated function environments. It has no GCP authentication, secrets or permission to deploy, and does not invoke suppliers or paid training. Dispatch it when runtime dependencies or component behavior change; do not add it to every documentation push.

## Dependency ownership

| Environment | Requirements / constraints | Validation |
| --- | --- | --- |
| Airflow | `airflow/requirements.txt`, `airflow/constraints-python311.txt` | Digest-pinned Python 3.11 image, pip check, application DAG import and actual task failure propagation |
| Beam | `pipelines/requirements.txt`, `pipelines/constraints-python311.txt` | Digest-pinned Python 3.11 image, pip check, real DirectRunner reads/transforms/writes with malformed and conflicting revisions |
| Each function | `functions/<feed>/requirements.txt`, adjacent `constraints-python311.txt` | Independently resolved Linux Python 3.11 graph, pip check, exact-version assertions and packaged entrypoint import |
| Local checks / cloud diagnostics | `requirements-dev.txt`, `requirements-ops.txt` | Separate environments, deliberately pinned direct dependencies; neither is a production runtime lock |

Airflow and Beam constraints preserve the exact dependency resolutions from their already validated 2026-10-28 images (source image IDs recorded in each file), rather than upgrading their SDKs. The function constraints are candidate resolutions of their existing requirement ranges on Linux Python 3.11. They become effective on the next controlled application deployment; existing cloud revisions are unchanged. All function ZIPs explicitly include their own constraints file. Keep environments separate: the Airflow provider and Beam have different compatible transitive requirements, so forcing one global lock would break isolation.

Constraints fix Python package versions, not package hashes, operating-system packages, the managed Cloud Run build toolchain or availability of historical artifacts. Container bases are pinned by digest; installed OS build tools still follow the distribution repositories. Retained validated runtime images remain the recovery source. Change a direct requirement and regenerate its environment's constraints together, review the diff, run that component workflow, then promote through the application deployment process. Do not silently regenerate constraints during deployment.

## Test paths and evidence

The new recovery handoff check runs the real archive producer, verifier, replay and push handler together, including a crash after sink acceptance and before ACK. Its IO is deliberately simulated; the production grouping SELECT is exercised through SQLite dialect adaptation. See [recovery evidence](recovery-demonstration.md).

`scripts/diagnostics/validate_airflow_runtime.py` imports the current nine-task DAG in a fresh SQLite metadatabase, checks its exact task identities, then runs an isolated two-task DAG: the source raises an Airflow exception, its task becomes failed, the downstream publication task becomes upstream_failed, and the run fails. It requires no GCP credentials and cannot publish. Set a fresh `isolated-*` Airflow home, matching SQLite connection, SequentialExecutor and `LOAD_EXAMPLES=False` as in the component workflow.

`scripts/diagnostics/validate_backfill_runtime.py` runs all three actual Beam graphs with temporary local fixtures and real file IO; production sink bindings are replaced at the IO boundary. Its `--recovery-cases --revision-cases` options exercise rejection evidence and conflicting revisions. Cloud mode additionally requires a fresh dataset in the guarded `runtime_validation_YYYYMMDD_8hex` namespace with matching ownership labels. Production destinations are rejected.

Existing small controlled GCP paths remain available; they are operator-run rather than credentialed PR jobs:

1. `validate_ingestion.py prepare/exercise/verify/cleanup --state <new receipt>` uses isolated function routes, subscriptions and dataset for actual Avro push-to-staging and sink failure/retry behavior. Retain the receipt and use its matching cleanup; never point it at serving staging.
2. `validate_atomic_delivery.py prepare/smoke/cleanup --receipt <new receipt>` creates guarded isolated staging, delivery, model and serving datasets. It exercises actual SQL quality checks, freeze and publication with synthetic facts. `exercise` additionally injects transactional failures, and `rollback` tests serving recovery. This path creates BigQuery ML test models and incurs query/training charges: reuse existing receipts unless publication semantics changed. Its per-query ceiling is 2 GiB, not a total monetary cap.
3. `validate_backfill_runtime.py --cloud-dataset <owned empty dataset>` verifies actual BigQuery/GCS loads. It is optional and must use the namespace and labels above; remove its dataset and scoped temporary grants afterwards.

These probes have already been exercised natively: [Python 3.11 source-to-publication validation](python311-runtime.md), [consumer cutover](ingestion-cutover.md), [atomic publication](atomic-analytics-release.md), and [current backfill/repair SQL validation](safe-backfill-repair.md). They cover real IO and SQL separately from credential-free CI. They are evidence of those revisions and probes, not proof that every future revision or hosted CI run passed. Item 31 does not require another paid end-to-end run merely to add dependency constraints and reusable component checks.

Hosted workflow execution and production promotion of the new candidate constraints are separate from local validation. The existing live runtime remains the accepted deployment until the controlled deployment gates pass.

Subsequent activation: item 33 promoted all five function constraints from the verified `237965d` application bundle. Serving archive members match those exact artifacts, while Python behavior and Avro schemas remain unchanged; see [security/deployment evidence](security-governance.md). The item 31 receipts below remain the original local validation record.

## Validation receipt, 2026-10-31

Both constrained images rebuilt locally and passed pip check. Airflow Python 3.11.10 imported all nine application tasks; the fault-injection DAG finished failed with source=failed and publish=upstream_failed. Beam Python 3.11.18 / 2.65.0 ran all three DirectRunner graphs with network disabled: five valid facts per feed, with cocoa/oil/weather rejection counts 6/5/7 and revision/evidence assertions passing. All five packaged functions installed in separate Linux Python 3.11.18 environments, passed pip check and exact resolved-version checks, and imported their entrypoints with network disabled (44/43/41/24/23 packages respectively). No supplier was invoked. Six focused packaging tests, targeted Ruff, workflow YAML parsing and diff whitespace checks passed. Private local logs are under `.build/item31/`.

The first Airflow probe used an outdated ten-task assumption; its assertion was corrected to the current exact task set before the successful run. An existing generated artifact directory correctly rejected unexpected files; validation used fresh `.build/item31-functions/` output instead. No existing artifacts were removed. Hosted CI, Dataflow workers and a fresh paid GCP end-to-end cycle were not run; previously recorded native evidence is reused as described above. No cloud resources were started, deployed or changed.
