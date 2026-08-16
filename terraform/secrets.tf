locals {
  secrets = toset([
    "FRED_OIL_API_KEY",
    "airflow-fernet-key",
  ])
}

resource "google_secret_manager_secret" "secrets" {
  for_each = local.secrets

  secret_id = each.key
  project   = var.project_id

  replication {
    auto {}
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_secret_manager_secret_iam_member" "airflow_accessor" {
  for_each = local.secrets

  project   = var.project_id
  secret_id = google_secret_manager_secret.secrets[each.key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${local.airflow_service_account}"
}
