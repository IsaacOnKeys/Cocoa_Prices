# GCP ownership and Terraform adoption

Verified 2026-10-27 after the owner-approved EIA cutover and legacy retirement.
The main root tracks 86 application resource instances; bootstrap tracks one
state bucket. Both roots returned exit 0 with live no-change plans. The EIA
function, trigger, schema-bound validation route and invocation/access grants
were applied, and the manually created EIA secret container was adopted.
The superseded function/trigger were retired after production acceptance; its
credential container is retained outside active Terraform ownership for rollback.
See the [cutover receipt](../operations/eia-cutover.md).

This evidence is stronger than a prototype configuration, but does not prove
that the original infrastructure was created through Terraform or that routine
deployments historically used it.

## What is tracked

| Root | Terraform resource type | Instances |
| --- | --- | ---: |
| main | `google_bigquery_dataset` | 4 |
| main | `google_bigquery_dataset_iam_member` | 3 |
| main | `google_cloud_run_v2_service_iam_member` | 4 |
| main | `google_cloud_scheduler_job` | 2 |
| main | `google_cloudfunctions2_function` | 3 |
| main | `google_compute_firewall` | 8 |
| main | `google_compute_instance` | 1 |
| main | `google_project_iam_member` | 32 |
| main | `google_pubsub_schema` | 3 |
| main | `google_pubsub_subscription` | 4 |
| main | `google_pubsub_topic` | 9 |
| main | `google_secret_manager_secret` | 2 |
| main | `google_secret_manager_secret_iam_member` | 2 |
| main | `google_service_account` | 3 |
| main | `google_storage_bucket` | 2 |
| main | `google_storage_bucket_iam_member` | 4 |
| bootstrap | `google_storage_bucket` | 1 |


The [refreshed inventory](gcp-resource-inventory.json) lists 87 managed addresses
and external IDs, including bootstrap, plus its exact historical baseline link.
It is not a Terraform state file. Credentials/private state remain outside Git.

## Service and ownership boundaries

| GCP service / component | Current ownership |
| --- | --- |
| Compute Engine | VM, eight application firewall rules and selected IAM in Terraform; default network/subnet referenced. Installed files, metadata bootstrap and runtime environments remain operator-owned. |
| Pub/Sub | Three Avro schemas, nine topics, four application subscriptions and application IAM in Terraform. DLQ topic existence does not establish dead-letter policies. |
| Cloud Functions Gen2 | Three functions and their runtime/trigger/resource settings in Terraform. Application deployments own source archive generations. |
| Cloud Run / Eventarc | Function-created services, event triggers and service subscriptions stay Gen2-owned. Terraform owns explicit Airflow invoker grants and the EIA Eventarc identity's service-scoped invoker grant. |
| BigQuery | Four datasets and additive dataset IAM in Terraform. Beam/SQL own tables, views, procedures, models and data. |
| Cloud Storage | Two application buckets and bucket IAM in main state; backend bucket in bootstrap. Historical snapshot objects and holds are data provenance artifacts, not Terraform-managed bucket policy. |
| Secret Manager | Two active secret containers and accessor IAM in Terraform; prior oil credential container retained externally for rollback. Secret payloads/versions remain outside source and state ownership. |
| Cloud Scheduler | Two jobs in Terraform: VM start at 18:30 and stop at 20:30, Europe/Berlin, daily. Airflow schedules the DAG at 19:00 Monday-Friday. |
| IAM / service accounts | Three custom service accounts and additive application grants tracked. Human access and Google service agents remain external. |
| Artifact Registry / Cloud Build | Current gcf-artifacts repository and function builds are Gen2-managed. No cocoa-code-project repository is currently deployed. Backfill Docker builds are a separate supported local workflow. |
| Dataflow | Historical runner support remains in the backfill sources. No new Dataflow job or infrastructure was deployed during review. |
| Logging / Monitoring | Available APIs and IAM are not proof of a configured alerting/observability stack; that remains engineering backlog work. |
| Looker Studio | Downstream visualization; report configuration is outside Terraform and this repository. |
| API enablement | Enabled-service inventory was inspected. Terraform deliberately does not manage project API enable/disable lifecycle. Enabled APIs alone do not prove application usage. |

## Adopting existing infrastructure after the fact

Terraform supports [importing existing infrastructure](https://developer.hashicorp.com/terraform/cli/import).
For this project, the state mappings already exist and refreshed plans match
them. Re-importing or recreating those resources would be unnecessary.

For a future ownership expansion: inventory an unmanaged object, establish its
owner, add compatible configuration/import blocks, review the proposed plan,
and approve the state/adoption change separately. Avoid taking duplicate
ownership of function-generated services or resources recreated by daily SQL.

The same backend bucket and prefixes are preserved:

~~~text
bucket: cocoa-prices-430315-terraform-state
main: terraform/main
bootstrap: terraform/bootstrap
workspace: default (both roots)
~~~

Terraform 1.15.8 and Google provider 7.44.0 were used; both existing lockfiles
are unchanged. Saved ADC failed refresh initially. Verification used the existing
gcloud session token in a process-only environment variable, supported by the
[provider](https://github.com/hashicorp/terraform-provider-google/blob/main/website/docs/guides/provider_reference.html.markdown)
and [GCS backend](https://developer.hashicorp.com/terraform/language/backend/gcs).
No token was printed or persisted; saved login configuration was not changed.

See [Terraform workflow](../../infra/terraform/README.md) and the
[infrastructure reference](terraform.md).
