locals {
  application_buckets = {
    cocoa-prices-temp-for-bq = {
      location = "EUROPE-WEST3"
    }
    raw-historic-data = {
      location = "EUROPE-WEST3"
    }
  }

  bucket_iam_members = {
    temp_compute_object_admin = {
      bucket = "cocoa-prices-temp-for-bq"
      role   = "roles/storage.objectAdmin"
      member = "serviceAccount:${local.compute_service_account}"
    }
    temp_airflow_object_viewer = {
      bucket = "cocoa-prices-temp-for-bq"
      role   = "roles/storage.objectViewer"
      member = "serviceAccount:${local.airflow_service_account}"
    }
    raw_compute_object_creator = {
      bucket = "raw-historic-data"
      role   = "roles/storage.objectCreator"
      member = "serviceAccount:${local.compute_service_account}"
    }
    raw_compute_object_viewer = {
      bucket = "raw-historic-data"
      role   = "roles/storage.objectViewer"
      member = "serviceAccount:${local.compute_service_account}"
    }
  }
}

resource "google_storage_bucket" "application" {
  for_each = local.application_buckets

  name                        = each.key
  project                     = var.project_id
  location                    = each.value.location
  storage_class               = "STANDARD"
  force_destroy               = false
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_storage_bucket_iam_member" "application" {
  for_each = local.bucket_iam_members

  bucket = google_storage_bucket.application[each.value.bucket].name
  role   = each.value.role
  member = each.value.member
}
