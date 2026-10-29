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

  dynamic "versioning" {
    for_each = each.key == "raw-historic-data" ? [true] : []
    content {
      enabled = true
    }
  }

  # Only new daily inputs expire. Historical data and held release artifacts
  # retain their existing policy. Versioned deletes remain recoverable for 30 days.
  dynamic "lifecycle_rule" {
    for_each = each.key == "raw-historic-data" ? {
      live       = { state = "LIVE", noncurrent_days = null }
      noncurrent = { state = "ARCHIVED", noncurrent_days = 30 }
    } : {}
    content {
      action {
        type = "Delete"
      }
      condition {
        age                        = 400
        with_state                 = lifecycle_rule.value.state
        days_since_noncurrent_time = lifecycle_rule.value.noncurrent_days
        matches_prefix             = ["daily-inputs/v1/"]
      }
    }
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
