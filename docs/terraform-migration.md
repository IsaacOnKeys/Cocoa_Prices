# Terraform existing-resource migration

Migration completed on 2026-08-16 for Google Cloud project
`cocoa-prices-430315`, primarily in `europe-west3`. The goal was adoption, not
recreation: every existing managed resource was imported before its final plan
was accepted.

## Result

- Main state: **83 existing resources and additive IAM entries imported**.
- Bootstrap state: **1 new private GCS state bucket created and managed**.
- Main plan: **No changes. Your infrastructure matches the configuration.**
- Bootstrap plan: **No changes. Your infrastructure matches the configuration.**
- Existing resources changed, replaced, stopped, or redeployed: **none**.
- Secret values read or stored in configuration: **none**.

The only cloud resource created during the migration was
`cocoa-prices-430315-terraform-state`, which was explicitly approved for remote
state. It has uniform bucket-level access, public-access prevention, versioning,
30-day soft delete, and `prevent_destroy`.

## Managed inventory

| Resource group | Count | Terraform ownership |
| --- | ---: | --- |
| Remote-state bucket | 1 | Separate bootstrap root |
| Custom service accounts | 3 | `api-fetcher`, `airflow-orchestrator`, `vm-scheduler-sa` |
| Compute Engine instances | 1 | `airflow-vm` in `europe-west3-c` |
| Custom firewall rules | 8 | Existing Airflow UI, SSH, and IAP rules |
| Application Cloud Storage buckets | 2 | `cocoa-prices-temp-for-bq`, `raw-historic-data` |
| Bucket IAM members | 4 | Additive application grants |
| BigQuery datasets | 4 | `cocoa_related`, `cocoa_related_studio`, `models`, `stream_staging` |
| BigQuery dataset IAM members | 3 | Additive Airflow writer grants |
| Pub/Sub schemas | 3 | Cocoa, oil, and weather Avro schemas |
| Pub/Sub topics | 8 | Data, dead-letter, and trigger topics |
| Pub/Sub subscriptions | 3 | Application pull subscriptions |
| Gen2 Cloud Functions | 3 | Cocoa, oil, and weather publishers |
| Cloud Run IAM members | 3 | Explicit Airflow invoker grants only |
| Cloud Scheduler jobs | 2 | Daily Airflow VM start and stop jobs |
| Secret Manager containers | 2 | Secret metadata only; no versions or payloads |
| Secret-level IAM members | 2 | Additive Airflow accessor grants |
| Project-level IAM members | 32 | Application service accounts only |

The exact Terraform addresses and import IDs are preserved in
[`terraform/imports.sh`](../terraform/imports.sh). Every command in that script
completed successfully during the migration.

## Intentionally outside Terraform

| Resource or configuration | Reason |
| --- | --- |
| Default VPC, auto-mode subnets, and default firewall rules | Project defaults; referenced through data sources rather than claimed |
| Human Owner, OS Login, Compute Viewer, and IAP IAM | Operator access is not application IAM |
| Google service agents and service-agent IAM | Created and maintained by Google services |
| Enabled Google APIs | Broad historical set; disabling one through Terraform could disrupt unrelated services |
| Container Analysis Pub/Sub topics | Google-generated service topics |
| Eventarc triggers and Eventarc subscriptions | Generated and owned by Gen2 Cloud Functions |
| Underlying Cloud Run services | Generated and owned by Gen2 Cloud Functions; only explicit IAM is managed |
| `gcf-artifacts` repository and GCF source/upload buckets | Generated and owned by Cloud Functions |
| Dataflow staging buckets | Service-created runtime/staging infrastructure |
| Dataflow jobs, Cloud Build jobs, images, and bucket objects | Transient application/runtime artifacts |
| BigQuery tables, views, procedures, models, and their data | Created by Beam and SQL application workflows |
| Secret versions and values | Sensitive runtime data must not enter Terraform state |
| Function source archive generations | Application deployments own code; Terraform ignores source drift to avoid rollback |
| VM metadata and filesystem contents | Contains externally managed SSH/bootstrap/runtime configuration |
| Default project groups and human BigQuery access | Non-application access remains outside additive Terraform IAM |

The batch scripts reference an Artifact Registry repository named
`cocoa-code-project`, but it was not deployed at inventory time and therefore
there was no resource to import.

## Migration and safety record

1. Repository code, DAGs, scripts, SQL, schemas, and architecture documents were
   inspected for GCP resource references.
2. Live resources were inventoried with read-only `gcloud` and `bq` commands.
   Cloud Asset Inventory was not enabled solely for discovery.
3. Terraform 1.15.8 was installed as a checksum-verified portable binary. The
   official Google provider 7.44.0 is recorded in both lock files.
4. The bootstrap plan showed `1 to add, 0 to change, 0 to destroy` and created
   only the approved state bucket. Bootstrap state was then migrated to GCS.
5. Existing resources and additive IAM entries were imported with CLI imports.
6. The first main plan was blocked by `prevent_destroy` when an imported VM
   field was not yet represented canonically. Nothing was applied.
7. Configuration was reconciled to the provider's canonical URLs and ownership
   boundaries. A later plan contained only three schema line-ending diffs.
8. Windows CRLF schema text was normalized to Pub/Sub's LF representation.
9. Final `terraform fmt -check`, `terraform validate`, and both plans succeeded
   with zero changes.

## Remaining local and future work

- Refresh ADC interactively before the next normal Terraform session:
  `gcloud auth application-default login`. The automated migration used only
  short-lived tokens from the already authenticated Google Cloud CLI after the
  existing ADC refresh token failed.
- The eight imported Airflow firewall rules contain historical duplicates.
  Consolidation may improve security and clarity, but it must be a separate,
  reviewed infrastructure change.
- API enablement can be migrated later if the project establishes a deliberate
  allowlist. Use destruction safeguards so removing configuration cannot disable
  an API unexpectedly.
- If `cocoa-code-project` is recreated for Dataflow images, add its repository
  through a reviewed Terraform plan before publishing images to it.

## Future infrastructure workflow

From `terraform/`, after ADC login:

```bash
terraform fmt
terraform validate
terraform plan
terraform apply
```

Commit configuration and `.terraform.lock.hcl`; never commit state, plan files,
private `.tfvars`, credentials, or secret values.
