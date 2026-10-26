locals {
  bigquery_datasets = toset([
    "cocoa_related",
    "cocoa_related_studio",
    "models",
    "stream_staging",
  ])

  airflow_writer_datasets = toset([
    "cocoa_related",
    "models",
    "stream_staging",
  ])
}

resource "google_bigquery_dataset" "datasets" {
  for_each = local.bigquery_datasets

  dataset_id                 = each.key
  project                    = var.project_id
  location                   = var.region
  delete_contents_on_destroy = false
  is_case_insensitive        = false
  max_time_travel_hours      = 168

  lifecycle {
    prevent_destroy = true

    # Default project groups and human access remain outside Terraform.
    # Application-specific access is managed additively below.
    ignore_changes = [access]
  }
}

resource "google_bigquery_dataset_iam_member" "airflow_writer" {
  for_each = local.airflow_writer_datasets

  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets[each.key].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${local.airflow_service_account}"
}
