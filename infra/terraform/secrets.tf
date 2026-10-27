locals {
  secrets = toset([
    "EIA_OIL_API_KEY",
    "airflow-fernet-key",
  ])
}

resource "google_secret_manager_secret" "secrets" {
  for_each = local.secrets

  secret_id = each.key
  project   = var.project_id
  labels = each.key == "EIA_OIL_API_KEY" ? {
    app       = "cocoa-prices"
    component = "oil-ingestion"
    source    = "eia"
  } : null

  replication {
    auto {}
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_secret_manager_secret_iam_member" "airflow_accessor" {
  for_each = setsubtract(local.secrets, toset(["EIA_OIL_API_KEY"]))

  project   = var.project_id
  secret_id = google_secret_manager_secret.secrets[each.key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${local.airflow_service_account}"
}

# Adopt only the manually created container; secret versions stay outside state.
import {
  to = google_secret_manager_secret.secrets["EIA_OIL_API_KEY"]
  id = "projects/${var.project_id}/secrets/EIA_OIL_API_KEY"
}

resource "google_secret_manager_secret_iam_member" "eia_oil_accessor" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.secrets["EIA_OIL_API_KEY"].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${local.compute_service_account}"
}
