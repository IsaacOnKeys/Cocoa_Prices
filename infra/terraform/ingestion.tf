variable "ingestion_validation_routes" {
  description = "Temporary isolated subscription routes used only during deployment validation."
  type = map(object({
    feed  = string
    table = string
  }))
  default = {}
}

variable "daily_ingestion_push_enabled" {
  description = "Enable the validated Cloud Run consumer on existing daily subscriptions."
  type        = bool
  default     = false
}

locals {
  daily_ingestion_feeds = {
    cocoa-prices-sub = { feed = "cocoa", table = "${var.project_id}.stream_staging.cocoa_prices" }
    oil-prices-sub   = { feed = "oil", table = "${var.project_id}.stream_staging.oil_prices" }
    weather-data-sub = { feed = "weather", table = "${var.project_id}.stream_staging.precipitation_moisture" }
  }
  daily_ingestion_routes = var.daily_ingestion_push_enabled ? {
    for subscription, route in local.daily_ingestion_feeds :
    "projects/${var.project_id}/subscriptions/${subscription}" => route
  } : {}
}

resource "google_service_account" "ingestion_runtime" {
  project      = var.project_id
  account_id   = "analytics-staging"
  display_name = "Bounded analytics staging consumer"

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_service_account" "ingestion_push" {
  project      = var.project_id
  account_id   = "analytics-push"
  display_name = "Authenticated PubSub delivery to analytics staging"

  lifecycle {
    prevent_destroy = true
  }
}

# Scope token minting to this delivery identity, rather than the project.
resource "google_service_account_iam_member" "ingestion_push_tokens" {
  service_account_id = google_service_account.ingestion_push.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${var.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_cloudfunctions2_function" "ingestion" {
  project  = var.project_id
  location = var.region
  name     = "stage_analytics_data"
  labels   = { app = "cocoa-prices", component = "staging-ingestion" }

  build_config {
    runtime           = "python312"
    entry_point       = "stage_analytics_data"
    docker_repository = local.function_repository
    service_account   = "projects/${var.project_id}/serviceAccounts/${local.compute_service_account}"

    automatic_update_policy {}
    source {
      storage_source {
        bucket     = local.function_source_bucket
        object     = "stage_analytics_data/ingestion-41acd6ddebaaa396974df7f0ee3435081336713ee65f42581a45adb61dc64a95.zip"
        generation = 1793134386133654
      }
    }
  }

  service_config {
    service_account_email            = google_service_account.ingestion_runtime.email
    min_instance_count               = 0
    max_instance_count               = 3
    available_memory                 = "512M"
    available_cpu                    = "1"
    max_instance_request_concurrency = 1
    timeout_seconds                  = 60
    ingress_settings                 = "ALLOW_ALL"
    all_traffic_on_latest_revision   = true
    environment_variables = {
      GCP_PROJECT      = var.project_id
      INGESTION_ROUTES = jsonencode(merge(local.daily_ingestion_routes, var.ingestion_validation_routes))
      LOG_EXECUTION_ID = "true"
    }
  }

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [build_config[0].source]
  }
}

resource "google_cloud_run_v2_service_iam_member" "ingestion_push_invoker" {
  project  = var.project_id
  location = var.region
  name     = "stage-analytics-data"
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.ingestion_push.email}"

  depends_on = [google_cloudfunctions2_function.ingestion]
}

resource "google_bigquery_dataset_iam_member" "ingestion_staging_writer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets["stream_staging"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.ingestion_runtime.email}"
}
