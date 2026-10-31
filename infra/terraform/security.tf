# Daily acquisition does not inherit the legacy build/backfill identity's roles.
resource "google_service_account" "producer_runtime" {
  project      = var.project_id
  account_id   = "analytics-producer"
  display_name = "Daily acquisition runtime"

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_pubsub_topic_iam_member" "producer_publish" {
  for_each = toset(["cocoa-prices-topic", "oil-prices-topic", "weather-topic"])
  project  = var.project_id
  topic    = google_pubsub_topic.topics[each.key].name
  role     = "roles/pubsub.publisher"
  member   = "serviceAccount:${google_service_account.producer_runtime.email}"
}

resource "google_storage_bucket_iam_member" "producer_raw" {
  for_each = toset(["roles/storage.objectCreator", "roles/storage.objectViewer"])
  bucket   = "raw-historic-data"
  role     = each.key
  member   = "serviceAccount:${google_service_account.producer_runtime.email}"
  condition {
    title      = "daily-acquisition-evidence"
    expression = "resource.name.startsWith('projects/_/buckets/raw-historic-data/objects/daily-inputs/v1/') || resource.name.startsWith('projects/_/buckets/raw-historic-data/objects/acquisition-coverage/v1/')"
  }
}

resource "google_storage_bucket_iam_member" "producer_coverage" {
  bucket = "cocoa-prices-temp-for-bq"
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.producer_runtime.email}"
  condition {
    title      = "daily-acquisition-state"
    expression = "resource.name.startsWith('projects/_/buckets/cocoa-prices-temp-for-bq/objects/acquisition-coverage/v1/')"
  }
}

resource "google_secret_manager_secret_iam_member" "producer_eia" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.secrets["EIA_OIL_API_KEY"].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.producer_runtime.email}"
}

resource "google_pubsub_topic_iam_member" "airflow_trigger_publish" {
  for_each = toset(["eia-oil-trigger", "weather-trigger"])
  project  = var.project_id
  topic    = google_pubsub_topic.topics[each.key].name
  role     = "roles/pubsub.publisher"
  member   = "serviceAccount:${local.airflow_service_account}"
}

# Exact receipt reads and manual Beam archive matching need the bucket's viewer/list permissions.
resource "google_storage_bucket_iam_member" "airflow_raw" {
  bucket = "raw-historic-data"
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${local.airflow_service_account}"
}

# These targeted allows have priority 1000; the unscoped default allows use 65534.
resource "google_compute_firewall" "airflow_admin_guard" {
  project       = var.project_id
  name          = "cocoa-airflow-deny-public-admin"
  network       = data.google_compute_network.default.name
  direction     = "INGRESS"
  priority      = 1100
  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["airflow-ui"]
  deny {
    protocol = "tcp"
    ports    = ["22", "3389", "8080"]
  }

  lifecycle {
    prevent_destroy = true
  }
}

# Narrow audit coverage; do not enable chargeable Data Access logs for all APIs.
resource "google_project_iam_audit_config" "secret_reads" {
  project = var.project_id
  service = "secretmanager.googleapis.com"
  audit_log_config {
    log_type = "DATA_READ"
  }
}
