# Free control-plane API, used by the pinned stable provider for daily query quotas.
resource "google_project_service" "cloud_quotas" {
  project            = var.project_id
  service            = "cloudquotas.googleapis.com"
  disable_on_destroy = false
}

# Native quota metric uses MiBy, not bytes. Applies to every user/service account.
# Normal daily Airflow queries use ~220 MiB; manual training/backfills need review.
provider "google" {
  alias                 = "quotas"
  project               = var.project_id
  billing_project       = var.project_id
  user_project_override = true
}

resource "google_cloud_quotas_quota_preference" "daily_queries" {
  provider = google.quotas
  for_each = {
    QueryUsagePerDay        = 65536
    QueryUsagePerUserPerDay = 5120
  }
  parent   = "projects/${var.project_number}"
  service  = "bigquery.googleapis.com"
  quota_id = each.key
  # Explicitly lower the huge defaults. Keep Google's below-recent-usage check:
  # project use reached 45510 MiB during development/training, hence 64 GiB.
  ignore_safety_checks = "QUOTA_DECREASE_PERCENTAGE_TOO_HIGH"
  justification        = "Bound CV-project query cost; 64 GiB project and 5 GiB per user daily. Project limit exceeds observed development peak; large training needs explicit review."
  quota_config {
    preferred_value = tostring(each.value)
  }
  depends_on = [google_project_service.cloud_quotas]
}
