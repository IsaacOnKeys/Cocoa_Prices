# Terraform infrastructure

Terraform defines the Google Cloud infrastructure for project
`cocoa-prices-430315`, primarily in `europe-west3`.

## Current status

The `cocoa-prices-430315-terraform-state` bucket provides the GCS backend. It
uses uniform bucket-level access, public-access prevention, versioning, 30-day
soft delete, and `prevent_destroy`. Main and bootstrap state use separate
prefixes.

## Managed inventory

| Resource group | Count | Terraform ownership |
| --- | ---: | --- |
| Remote-state bucket | 1 | Separate bootstrap root |
| Custom service accounts | 3 | `api-fetcher`, `airflow-orchestrator`, `vm-scheduler-sa` |
| Compute Engine instances | 1 | `airflow-vm` in `europe-west3-c` |
| Custom firewall rules | 8 | Airflow UI, SSH, and IAP rules |
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

## Ownership boundaries

| Resource or configuration | Ownership decision |
| --- | --- |
| Default VPC, auto-mode subnets, and default firewall rules | Referenced through data sources rather than managed directly |
| Human Owner, OS Login, Compute Viewer, and IAP IAM | Operator access is separate from application IAM |
| Google service agents and service-agent IAM | Maintained by Google services |
| Enabled Google APIs | Kept outside the current module to prevent accidental service disablement |
| Container Analysis Pub/Sub topics | Maintained by the Google service |
| Eventarc triggers and Eventarc subscriptions | Owned by Gen2 Cloud Functions |
| Underlying Cloud Run services | Owned by Gen2 Cloud Functions; only explicit IAM is managed |
| `gcf-artifacts` repository and GCF source/upload buckets | Owned by Cloud Functions |
| Dataflow staging buckets | Service runtime and staging infrastructure |
| Dataflow jobs, Cloud Build jobs, images, and bucket objects | Transient application/runtime artifacts |
| BigQuery tables, views, procedures, models, and their data | Owned by Beam and SQL application workflows |
| Secret versions and values | Sensitive runtime data must not enter Terraform state |
| Function source archive generations | Application deployments own code; source drift is ignored to prevent rollback |
| VM metadata and filesystem contents | Externally managed SSH, bootstrap, and runtime configuration |
| Default project groups and human BigQuery access | Separate from additive application IAM |

The batch scripts reference an Artifact Registry repository named
`cocoa-code-project`. It is not currently deployed, so it is not part of the
Terraform resource graph.

## Safety controls

- Remote state is private, versioned, and protected with soft delete.
- Application IAM uses additive member resources instead of authoritative
  project policies.
- High-value resources use `prevent_destroy`.
- Cloud Function source changes remain in the application deployment workflow.
- Secret payloads are never represented in Terraform.
- Pub/Sub schema text normalizes Windows CRLF endings to the LF representation
  used by the service.
- Provider-generated attribution labels are disabled so labels stay explicit.

## Verification

Verified on 2026-08-16 with:

- Terraform 1.15.8
- Google provider 7.44.0, recorded in both dependency lock files
- `terraform fmt -check`
- `terraform validate`
- Main and bootstrap `terraform plan`

Both plans returned zero changes.

## Operational notes

- Authenticate a workstation with
  `gcloud auth application-default login` before running Terraform.
- The eight Airflow firewall rules can be consolidated in a separate reviewed
  infrastructure change if the access model is simplified.
- API ownership can be added later if the project establishes a deliberate
  allowlist and destruction safeguards.
- If `cocoa-code-project` is created for Dataflow images, define its repository
  in Terraform before publishing images to it.

## Infrastructure workflow

From `terraform/`:

```bash
terraform fmt
terraform validate
terraform plan
terraform apply
```

Commit configuration and `.terraform.lock.hcl`; never commit state, plan files,
private `.tfvars`, credentials, or secret values.
