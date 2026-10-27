# Runtime and repository-to-install mappings

Release update, 2026-10-27: the cocoa/weather consumers were installed from
merged commit 553adb0, with checksummed source recovery copies retained. All
ten tasks in a controlled daily DAG run succeeded. The repository SQL was
reconciled to the existing deployed definition without redeploying the
procedure. See the [release validation](release-validation-2026-10-27.md).
The captures below describe the initial 2026-10-26 inspection.

Captured on 2026-10-26 during an explicitly authorized, bounded VM inspection.
Source organization does not install code or change existing VM filenames.

## Scheduling and lifecycle

Cloud Scheduler starts airflow-vm daily at 18:30 and stops it at 20:30,
Europe/Berlin. The Airflow DAG runs at 19:00 Monday-Friday. These are separate
scheduling responsibilities. The Beam services are enabled under systemd and
start with the VM; the DAG triggers producers, waits for progress, and runs SQL.

Use gcloud compute ssh airflow-vm --zone=europe-west3-c --project=cocoa-prices-430315
to resolve the VM's current address. Its external IP changes on restart; a
hard-coded SSH alias can become stale.

## Captured service mappings

| Repository source | Installed target / runtime owner |
| --- | --- |
| pipelines/streaming/cocoa_stream.py | /opt/beam/cocoa/local_cocoa_stream.py; beam-cocoa.service |
| pipelines/streaming/oil_stream.py | /opt/beam/oil/local_oil_stream.py; beam-oil.service |
| pipelines/streaming/weather_stream.py | /opt/beam/weather/local_weather_stream.py; beam-weather.service |
| `schemas/<feed>_schema.avsc` | `/opt/beam/<feed>`/`schemas/<feed>_schema.avsc` |
| airflow/dags/cocoa_daily.py | /opt/airflow/dags/cocoa_daily_dag.py, bind-mounted at /opt/airflow/dags in Airflow containers |
| scripts/operations/setup_fernet_secret.sh | Installer generates /opt/airflow/bin/setup_fernet_secret.sh; not executed during review |
| scripts/operations/restart_with_secrets.sh | Installer generates /opt/airflow/bin/restart_with_secrets.sh; not executed during review |

Actual Beam WorkingDirectory is `/opt/beam/<feed>`, rather than the shared root
described in older notes. All three units use User/Group beam, the interpreter
/opt/beam/venv/bin/python, EnvironmentFile=/opt/beam/.env, Restart=always and
RestartSec=5. No drop-ins were reported. Python 3.11.2 and Beam 2.65.0 were captured.

The [four captured systemd units](../../infra/systemd/) retain real installed
paths. Secret environment values are omitted. Repository filenames can change
while these installed names remain stable. No copy/install was performed.

## Docker and Airflow

Docker 28.3.3 and Compose v2.39.1 were captured. Airflow uses
apache/airflow:2.10.2, LocalExecutor and postgres:13. The separate historical
[backfill Dockerfile](../../pipelines/backfill/Dockerfile) is not this deployment.

[docker-compose.yaml](../../airflow/docker-compose.yaml) is a sanitized capture
of the real expanded Compose structure, expressed as JSON-compatible YAML.
Secret-bearing environment values are replaced with required external
references. It is reference material, not a claim of self-contained deployment.
Live paths are /opt/airflow/{dags,logs,plugins}; PostgreSQL uses its existing
named volume. No checkout bind mount was found.

Captured [component requirements](../../airflow/requirements.txt) record Airflow
2.10.2, Google provider 10.22.0 and Pendulum 3.0.0 (Python 3.12.6 in the image).
Baseline and relocated repository DAG sources were parsed in a disposable,
network-isolated copy of that image with Variable.get supplied from defaults.
The ten-task graph, IDs, schedule, timezone and trigger rules matched.
No producer, BigQuery task, or production DAG run was manually triggered.
This parse check uses a representative default configuration, not a claim
that all deployed Variables or source archives match the repository.

The installed DAG and oil consumer match the inspected repository counterparts
after line-ending normalization. Follow-up reconciliation uses deployed row
processing as the reference: the repository cocoa mapper now preserves and
normalizes producer ingestion_time using the deployed _to_ts implementation.
Weather retains receipt-time mapping and now accepts its deployed WEATHER_SUB
and WEATHER_TABLE aliases alongside the repository aliases.

Existing repository project, subscription, table and schema-path overrides
remain available. For weather, deployed aliases take precedence:
WEATHER_SUB -> WEATHER_SUBSCRIPTION -> PUBSUB_SUBSCRIPTION_WEATHER, and
WEATHER_TABLE -> WEATHER_BQ_TABLE -> BQ_TABLE_WEATHER. Defaults, tables, schemas,
DirectRunner/streaming options and installed filenames are unchanged.

All three row mapper ASTs match the captured VM versions, and populated/nullable
canonical Avro fixtures produce matching decoded records and rows. The source
files deliberately also contain the documented repository compatibility options;
whole-file byte equality is not claimed. No consumer source was installed on
the VM during reconciliation. See the
[reliability handoff](reliability-handoff-2026-10-26.md) for authority and evidence.

The startup test and final service/VM state are recorded in the
[portfolio handoff](portfolio-handoff-2026-10-26.md).

## Functions and artifacts

All three deployed functions are Gen2, python311, in europe-west3.

| Source | Prepared root | Preserved entry point | Actual trigger |
| --- | --- | --- | --- |
| functions/cocoa/ | .build/functions/cocoa/ | publish_price | HTTP |
| functions/oil/ | .build/functions/oil/ | publish_fred_data | oil-trigger |
| functions/weather/ | .build/functions/weather/ | publish_weather_data | weather-trigger |

Future source-directory deployments must use the prepared root, which includes
the canonical schema beside main.py. Separate requirements and all existing
deployment/resource/authentication contracts are preserved. Terraform ignores
function source drift. Cocoa/weather local artifacts were not asserted equal
to deployed ZIPs. The deployed oil source files match the approved artifact.

Observed deployed source generations are 1757954320101566 (cocoa),
1793036345024607 (oil, logging fix deployed 2026-10-26), and
1759406696390100 (weather), under
`gcf-v2-sources-494039866722-europe-west3/<entry_point>/function-source.zip`.
Historical notes referring to oil/weather data topics as invocation triggers
are outdated; data publication topics remain unchanged.

## Local helpers

scripts/development/stream_start and stream_stop resolve the checkout root;
root logs/PID files and ./schemas fallbacks remain valid. The original local
PID file was not used for testing or removed. Tests used a temporary checkout,
stub consumers and stub kill calls. stream_status retains its Python-process listing.

The diagnostic and installer snippets perform real external work if invoked;
neither was executed as a validation check. No secret payload was captured.
