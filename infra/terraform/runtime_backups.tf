resource "google_storage_bucket" "runtime_backups" {
  name                        = "${var.project_id}-runtime-backups"
  project                     = var.project_id
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = { app = "cocoa-prices", purpose = "runtime-recovery" }

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  # Daily encrypted state expires; reusable image archives are bounded by the
  # backup tool's four-image/two-GiB-per-image ceiling and reviewed cleanup.
  lifecycle_rule {
    action { type = "Delete" }
    condition {
      age            = 14
      matches_prefix = ["runtime/"]
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_storage_bucket_iam_member" "runtime_backups" {
  for_each = toset(["roles/storage.objectCreator", "roles/storage.objectViewer"])
  bucket   = google_storage_bucket.runtime_backups.name
  role     = each.value
  member   = "serviceAccount:${local.airflow_service_account}"
}
