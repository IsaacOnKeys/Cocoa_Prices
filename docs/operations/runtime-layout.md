# Runtime layout and ownership

EIA serves production following the verified 2026-10-27 controlled cutover.
The superseded function/trigger are retired; source/credential recovery material
is retained. See the [deployment receipt](eia-cutover.md),
[migration evidence](eia-migration.md) and [service requirements](service-contract.md).
Daily staging uses authenticated Pub/Sub push into the bounded
stage_analytics_data Cloud Run function. Beam is retained for backfills/local
checks and emergency rollback; its three VM startup units are disabled.
See the [consumer handoff](ingestion-cutover.md). One passing cycle does not
establish the target service guarantees.

## Stable VM and orchestration interfaces

The retained, disabled Beam unit references use User/Group beam, interpreter
`/opt/beam/venv/bin/python`, EnvironmentFile `/opt/beam/.env`, Restart=always and
RestartSec=5. Its Python 3.11.2 and Beam 2.65.0 were captured in the baseline.
The EIA oil payload preserves the existing Avro fields and requires no provider
rename inside these preserved rollback paths:

| Repository source | Installed target |
| --- | --- |
| pipelines/streaming/cocoa_stream.py | /opt/beam/cocoa/local_cocoa_stream.py |
| pipelines/streaming/oil_stream.py | /opt/beam/oil/local_oil_stream.py |
| pipelines/streaming/weather_stream.py | /opt/beam/weather/local_weather_stream.py |
| schemas/<feed>_schema.avsc | /opt/beam/<feed>/schemas/<feed>_schema.avsc |
| airflow/dags/cocoa_daily.py | /opt/airflow/dags/cocoa_daily_dag.py |

Cloud Scheduler owns VM startup/shutdown at 18:30/20:30 Europe/Berlin daily;
the Airflow DAG schedules 19:00 Monday-Friday. The existing captured Compose
runtime uses Airflow 2.10.2, LocalExecutor and postgres:13. Actual installed
configuration, secrets and metadata must be checked at cutover; a Git branch
does not isolate deployed resources or change Airflow Variables.

Repository helper scripts resolve the checkout root. The systemd unit files
are installation/rollback references; daily startup no longer enables Beam.
Moving a source file does not install it on the VM.
The captured Compose definition requires external secret values and is not a
claim that the runtime can be rebuilt without the remaining deployment work.

## Managed daily consumer

functions/ingestion/ packages to .build/functions/ingestion/, containing the
handler/requirements and all three canonical schemas. stage_analytics_data is
a Python 3.12 Gen2 HTTP function in europe-west3. It runs independently of the
VM with one CPU, 512M memory, concurrency one, maximum three instances,
minimum zero and a 60-second timeout. Cloud Run IAM requires authentication.

The existing cocoa-prices-sub, oil-prices-sub and weather-data-sub subscriptions
use authenticated push to that function. Their topic identities and seven-day
retention are preserved; acknowledgement deadlines are 60 seconds. A delivery
gets a successful response only after its staging insert is accepted.
Runtime analytics-staging and delivery analytics-push identities are distinct;
the runtime gets dataset-scoped stream_staging writer access, and Pub/Sub's
token-minting grant is scoped to the delivery identity. No new project-wide
runtime grant is needed. See [consumer boundaries](../../functions/ingestion/README.md)
for retry, timestamp and exactly-once limitations.

## EIA function and routing

| Interface | Serving configuration |
| --- | --- |
| Source/prepared root | functions/oil/ -> .build/functions/oil/ |
| Function / entry point | publish_eia_data / publish_eia_data |
| Runtime/region | Gen2 Python 3.11 / europe-west3 |
| Trigger | eia-oil-trigger, invoked through Eventarc |
| API credential | Secret Manager EIA_OIL_API_KEY; values/versions remain outside Git and Terraform state ownership |
| Isolated output topic/subscription | eia-oil-validation / eia-oil-validation-sub |
| Production output | oil-prices-topic / oil-prices-sub; active output of publish_eia_data |
| Canonical wire schema | schemas/oil_schema.avsc; existing date/oil_price/ingestion_time/raw_payload fields |
| Warehouse interface | stream_staging.oil_prices and existing curated key/date semantics |

The repository and installed DAG default OIL_TRIGGER_TOPIC to eia-oil-trigger.
The deployed Variable was absent at cutover. Recheck overrides on deployment;
a repository default does not establish the effective deployed route.
The validation topic remains available, but testing it requires an isolated
producer configuration; it is not a second output of the serving function.

Eventarc requires its trigger service account to have roles/run.invoker on
the new function's Cloud Run service. The isolated cloud test found HTTP 403
when only the Airflow account had that grant; Terraform now includes the
service-scoped trigger-account grant. Secret access, topic publishing and
consumer permissions are distinct requirements and need runtime verification.

Function packages contain main.py, requirements.txt and a byte-identical oil
schema. Deploy from the prepared root/archive. Terraform ignores subsequent
application source drift; do not use a source generation from a different
function or silently claim a new source was deployed because the repository
changed. Deployment evidence must compare the actual serving archive members.

The cocoa and weather entry points and their generic installed interfaces
remain covered by the existing packager and their own release records. The
historical [production validation](release-validation-2026-10-27.md) records the
earlier controlled daily cycle, not an EIA cutover.

## Historical reference and rollback

Exact baseline resource names, source generations, original supplier archives,
runtime captures and recovery receipts are preserved in Git commit
`8b2861236dcc7fbe6ac28bd59f8d67960ec8928b` and the original held cloud objects.
The [historical handoff](portfolio-handoff-2026-10-26.md) points to that unchanged
record. Do not rename those historical identities to imply an EIA deployment.

The completed cutover records routing, task/query results and recovery material
in its [receipt](eia-cutover.md). The old producer was disabled before EIA
promotion and retired only after acceptance. Safe historical backfill and
production-wide quality gates remain their separate checklist items.
