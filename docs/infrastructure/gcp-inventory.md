# GCP ownership and Terraform adoption

Verified on 2026-10-26 against existing GCS state and live, refreshed Terraform
plans. The main root tracks 83 application resource instances; the bootstrap
root tracks the state bucket. Both roots returned exit 0 with no changes before
and after relocation to infra/terraform/. Nothing was imported or applied.

This evidence is stronger than a prototype configuration, but does not prove
that the original infrastructure was created through Terraform or that routine
deployments historically used it.

## What is tracked

| Root | Terraform resource type | Instances |
| --- | --- | ---: |
| main | `google_bigquery_dataset` | 4 |
| main | `google_bigquery_dataset_iam_member` | 3 |
| main | `google_cloud_run_v2_service_iam_member` | 3 |
| main | `google_cloud_scheduler_job` | 2 |
| main | `google_cloudfunctions2_function` | 3 |
| main | `google_compute_firewall` | 8 |
| main | `google_compute_instance` | 1 |
| main | `google_project_iam_member` | 32 |
| main | `google_pubsub_schema` | 3 |
| main | `google_pubsub_subscription` | 3 |
| main | `google_pubsub_topic` | 8 |
| main | `google_secret_manager_secret` | 2 |
| main | `google_secret_manager_secret_iam_member` | 2 |
| main | `google_service_account` | 3 |
| main | `google_storage_bucket` | 2 |
| main | `google_storage_bucket_iam_member` | 4 |
| bootstrap | `google_storage_bucket` | 1 |


The [sanitized resource inventory](gcp-resource-inventory.json) lists every
resource address and external ID. It is not a Terraform state file: credentials,
private firewall values, outputs and resource attribute payloads are omitted.

## Service and ownership boundaries

| GCP service / component | Current ownership |
| --- | --- |
| Compute Engine | VM, eight application firewall rules and selected IAM in Terraform; default network/subnet referenced. Installed files, metadata bootstrap and runtime environments remain operator-owned. |
| Pub/Sub | Three Avro schemas, eight topics, three application subscriptions and application IAM in Terraform. DLQ topic existence does not establish dead-letter policies. |
| Cloud Functions Gen2 | Three functions and their runtime/trigger/resource settings in Terraform. Application deployments own source archive generations. |
| Cloud Run / Eventarc | Function-created services, event triggers and service subscriptions stay Gen2-owned. Terraform owns only explicit Airflow invoker grants. |
| BigQuery | Four datasets and additive dataset IAM in Terraform. Beam/SQL own tables, views, procedures, models and data. |
| Cloud Storage | Two application buckets and bucket IAM in main state; backend bucket in bootstrap. Historical snapshot objects and holds are data provenance artifacts, not Terraform-managed bucket policy. |
| Secret Manager | Two secret containers and accessor IAM in Terraform. Secret payloads/versions remain outside source and state ownership. |
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
