# Runtime layout and EIA migration boundaries

The EIA migration uses a separate deployed candidate and isolated output route.
The original production producer and installed DAG remain in place until the
controlled cutover is verified. See [migration evidence](eia-migration.md) for
the active checkpoint and [service requirements](service-contract.md) for target
behavior rather than measured guarantees.

## Stable VM and orchestration interfaces

The captured VM uses enabled systemd services with User/Group beam, interpreter
`/opt/beam/venv/bin/python`, EnvironmentFile `/opt/beam/.env`, Restart=always and
RestartSec=5. Its Python 3.11.2 and Beam 2.65.0 were captured in the baseline.
The EIA oil payload preserves the existing Avro fields and requires no provider
rename inside these generic installed paths:

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
are installation references; moving a source file does not install it on the VM.
The captured Compose definition requires external secret values and is not a
claim that the runtime can be rebuilt without the remaining deployment work.

## EIA function and routing

| Interface | Candidate configuration |
| --- | --- |
| Source/prepared root | functions/oil/ -> .build/functions/oil/ |
| Function / entry point | publish_eia_data / publish_eia_data |
| Runtime/region | Gen2 Python 3.11 / europe-west3 |
| Trigger | eia-oil-trigger, invoked through Eventarc |
| API credential | Secret Manager EIA_OIL_API_KEY; values/versions remain outside Git and Terraform state ownership |
| Isolated output topic/subscription | eia-oil-validation / eia-oil-validation-sub |
| Intended production output | oil-prices-topic / oil-prices-sub, after accepted cutover |
| Canonical wire schema | schemas/oil_schema.avsc; existing date/oil_price/ingestion_time/raw_payload fields |
| Warehouse interface | stream_staging.oil_prices and existing curated key/date semantics |

The repository DAG now defaults OIL_TRIGGER_TOPIC to eia-oil-trigger. A deployed
Variable can override that default; the actual value and installed DAG must
be reconciled during cutover. A separate trigger prevents both old and new
producers from consuming one invocation while the candidate is being tested.

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

The EIA migration keeps the legacy producer available during validation.
Cutover must record the old/new trigger and DAG mappings, verify only the
intended producer is invoked, preserve a rollback path and test the resulting
warehouse/report outputs. Safe historical backfill and production-wide quality
gates remain their separate checklist items.
