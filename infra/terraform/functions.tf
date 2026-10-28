locals {
  cloud_functions = {
    publish_price = {
      entry_point       = "publish_price"
      source_object     = "publish_price/bounded-df6caeb6fe5f8c676466e28290c6a6e942218ecaf51fe24f0650edfe966f9d6b.zip"
      source_generation = 1793223073388889
      trigger_topic     = null
    }
    # Serving EIA producer; cutover evidence: docs/operations/eia-cutover.md.
    publish_eia_data = {
      entry_point       = "publish_eia_data"
      source_object     = "publish_eia_data/bounded-f12a183f65a713c427625a4757900e3bd99cb75665ca73f4e88d2f54abdd316f.zip"
      source_generation = 1793223074257112
      trigger_topic     = "eia-oil-trigger"
    }
    publish_weather_data = {
      entry_point       = "publish_weather_data"
      source_object     = "publish_weather_data/bounded-4dfb0858edd15d2ad561f9da147bc616c323b88792dddceeb4351bc02c0050d5.zip"
      source_generation = 1793223074876944
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

    environment_variables = merge({
      LOG_EXECUTION_ID = "true"
      }, each.key == "publish_eia_data" ? {
      # Owner-approved cutover; legacy invocation is disabled before promotion.
      OIL_TOPIC = "oil-prices-topic"
    } : {})
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

    # Source generations are promoted explicitly with verified serving bytes.
    # Ignoring source does not prevent stale artifacts during build changes.
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

# Eventarc invokes Gen2 functions as the trigger service account, not as Airflow.
resource "google_cloud_run_v2_service_iam_member" "eia_trigger_invoker" {
  project  = var.project_id
  location = var.region
  name     = "publish-eia-data"
  role     = "roles/run.invoker"
  member   = "serviceAccount:${local.compute_service_account}"

  depends_on = [google_cloudfunctions2_function.functions]
}
