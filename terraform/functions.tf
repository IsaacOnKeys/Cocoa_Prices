locals {
  cloud_functions = {
    publish_price = {
      entry_point       = "publish_price"
      source_object     = "publish_price/function-source.zip"
      source_generation = 1757954320101566
      trigger_topic     = null
    }
    publish_fred_data = {
      entry_point       = "publish_fred_data"
      source_object     = "publish_fred_data/function-source.zip"
      source_generation = 1758719904960528
      trigger_topic     = "oil-trigger"
    }
    publish_weather_data = {
      entry_point       = "publish_weather_data"
      source_object     = "publish_weather_data/function-source.zip"
      source_generation = 1759406696390100
      trigger_topic     = "weather-trigger"
    }
  }

  function_source_bucket = "gcf-v2-sources-${var.project_number}-${var.region}"
  function_repository    = "projects/${var.project_id}/locations/${var.region}/repositories/gcf-artifacts"
}

resource "google_cloudfunctions2_function" "functions" {
  for_each = local.cloud_functions

  name     = each.key
  project  = var.project_id
  location = var.region

  build_config {
    runtime           = "python311"
    entry_point       = each.value.entry_point
    docker_repository = local.function_repository
    service_account   = "projects/${var.project_id}/serviceAccounts/${local.compute_service_account}"

    automatic_update_policy {}

    source {
      storage_source {
        bucket     = local.function_source_bucket
        object     = each.value.source_object
        generation = each.value.source_generation
      }
    }
  }

  service_config {
    max_instance_count               = 100
    available_memory                 = "256M"
    timeout_seconds                  = 60
    max_instance_request_concurrency = 1
    available_cpu                    = "0.1666"
    ingress_settings                 = "ALLOW_ALL"
    all_traffic_on_latest_revision   = true
    service_account_email            = local.compute_service_account

    environment_variables = {
      LOG_EXECUTION_ID = "true"
    }
  }

  dynamic "event_trigger" {
    for_each = each.value.trigger_topic == null ? [] : [each.value.trigger_topic]
    content {
      trigger_region        = var.region
      event_type            = "google.cloud.pubsub.topic.v1.messagePublished"
      pubsub_topic          = google_pubsub_topic.topics[event_trigger.value].id
      retry_policy          = "RETRY_POLICY_DO_NOT_RETRY"
      service_account_email = local.compute_service_account
    }
  }

  lifecycle {
    prevent_destroy = true

    # Application deployments own source archives. Terraform owns the
    # function's infrastructure and must not roll code backward.
    ignore_changes = [build_config[0].source]
  }
}

resource "google_cloud_run_v2_service_iam_member" "airflow_invoker" {
  for_each = local.cloud_functions

  project  = var.project_id
  location = var.region
  name     = replace(each.key, "_", "-")
  role     = "roles/run.invoker"
  member   = "serviceAccount:${local.airflow_service_account}"

  depends_on = [google_cloudfunctions2_function.functions]
}
