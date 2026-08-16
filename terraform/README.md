# Terraform

This directory manages the existing Google Cloud infrastructure for project
`cocoa-prices-430315`. The resources were imported in place; application data
and deployed services were not rebuilt during adoption.

## Structure

- The current directory is the main infrastructure root. Its remote state is
  stored at prefix `terraform/main`.
- [`bootstrap/`](bootstrap/) manages the private, versioned GCS state bucket.
  Its remote state is stored at prefix `terraform/bootstrap` in that bucket.
- [`imports.sh`](imports.sh) is the idempotent audit record of the one-time
  existing-resource imports.
- `terraform.tfvars` contains the current operator firewall CIDR and is ignored.
  [`terraform.tfvars.example`](terraform.tfvars.example) is safe to commit.

## Prerequisites

- Terraform `>= 1.10, < 2.0`
- Google Cloud CLI authenticated to `cocoa-prices-430315`
- Google Application Default Credentials (ADC)

From Git Bash:

```bash
gcloud auth application-default login
gcloud config set project cocoa-prices-430315
terraform version
```

For the portable Windows binary installed during migration, start at the
repository root and add it to the current Git Bash session:

```bash
export PATH="$PWD/.tools/terraform:$PATH"
terraform version
```

The `.tools/` directory is local and ignored; a normal system installation is
preferred on a fresh workstation.

## Normal workflow

Run from `terraform/`:

```bash
terraform fmt
terraform validate
terraform plan
terraform apply
```

Only apply a saved or freshly reviewed plan. If a plan proposes replacement or
deletion of an adopted resource, stop and reconcile configuration first.

Run the backend check separately from `terraform/bootstrap/`:

```bash
terraform fmt
terraform validate
terraform plan
```

## Ownership boundaries

Terraform owns application infrastructure and additive application IAM grants.
It does not own secret payloads, BigQuery data or SQL-created objects, function
source deployments, Airflow runtime configuration, VM filesystem contents,
Google-managed service resources, human IAM, or transient jobs and builds.

Gen2 Cloud Functions own their generated Eventarc subscriptions, Eventarc
triggers, Cloud Run services, staging buckets, and Artifact Registry repository.
Terraform manages the functions themselves and only the explicit Airflow
invoker grants on the generated Cloud Run services.

The Airflow VM's metadata is intentionally ignored because it includes
externally managed SSH entries and runtime bootstrap settings. Destructive
resources use `prevent_destroy` as a migration safety guard.

See the [migration record](../docs/terraform-migration.md) for the complete
inventory, exclusions, import evidence, and final plan results.
