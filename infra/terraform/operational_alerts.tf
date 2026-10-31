variable "operator_email" {
  description = "Explicit alert recipient; configure in ignored terraform.tfvars."
  type        = string
  default     = null
}

variable "budget_billing_account" {
  description = "Billing account ID for the project-only budget; configure outside Git."
  type        = string
  default     = null
}

variable "budget_currency" {
  type    = string
  default = "EUR"
}

# Account-level budget APIs still require an explicit consumer/quota project.
provider "google" {
  alias                 = "budget"
  project               = var.project_id
  billing_project       = var.project_id
  user_project_override = true
}

resource "google_project_service" "billing_alert_apis" {
  for_each           = toset(["cloudbilling.googleapis.com", "billingbudgets.googleapis.com"])
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_monitoring_notification_channel" "operator" {
  count        = var.operator_email == null ? 0 : 1
  project      = var.project_id
  display_name = "Cocoa operator email"
  type         = "email"
  labels       = { email_address = var.operator_email }
}

locals {
  operator_channels = google_monitoring_notification_channel.operator[*].name
}

resource "google_billing_budget" "project_guardrail" {
  provider        = google.budget
  count           = var.budget_billing_account == null || var.operator_email == null ? 0 : 1
  billing_account = var.budget_billing_account
  display_name    = "Cocoa: target under EUR 5, maximum EUR 10"
  budget_filter {
    projects               = ["projects/${var.project_number}"]
    calendar_period        = "MONTH"
    credit_types_treatment = "EXCLUDE_ALL_CREDITS"
  }
  amount {
    specified_amount {
      currency_code = var.budget_currency
      units         = "10"
    }
  }
  dynamic "threshold_rules" {
    for_each = [0.5, 0.8, 1.0]
    content {
      threshold_percent = threshold_rules.value
      spend_basis       = "CURRENT_SPEND"
    }
  }
  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "FORECASTED_SPEND"
  }
  all_updates_rule {
    monitoring_notification_channels = local.operator_channels
    disable_default_iam_recipients   = true
  }
  depends_on = [google_project_service.billing_alert_apis]
}

resource "google_service_account" "watchdog" {
  project      = var.project_id
  account_id   = "analytics-watchdog"
  display_name = "Read-only analytics watchdog"
}

resource "google_project_iam_custom_role" "watchdog" {
  project     = var.project_id
  role_id     = "analyticsWatchdog"
  title       = "Read-only analytics watchdog"
  permissions = ["bigquery.jobs.create", "compute.instances.get", "logging.logEntries.list", "monitoring.timeSeries.list", "resourcemanager.projects.get", "serviceusage.services.use"]
}

resource "google_project_iam_member" "watchdog" {
  project = var.project_id
  role    = google_project_iam_custom_role.watchdog.name
  member  = "serviceAccount:${google_service_account.watchdog.email}"
}

resource "google_bigquery_dataset_iam_member" "watchdog" {
  for_each   = toset(["analytics_delivery", "cocoa_related_studio"])
  project    = var.project_id
  dataset_id = each.value
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.watchdog.email}"
}

resource "google_cloudfunctions2_function" "watchdog" {
  project  = var.project_id
  location = var.region
  name     = "check_analytics_delivery"
  labels   = { app = "cocoa-prices", component = "watchdog" }
  build_config {
    runtime           = "python311"
    entry_point       = "check_analytics_delivery"
    docker_repository = local.function_repository
    service_account   = "projects/${var.project_id}/serviceAccounts/${local.compute_service_account}"
    automatic_update_policy {}
    source {
      storage_source {
        bucket     = local.function_source_bucket
        object     = "check_analytics_delivery/locked-6bb49e50a814788ea39aaa938074c00a835f55d28ae37c6f44b1a7f469e960bc.zip"
        generation = 1793443245510458
      }
    }
  }
  service_config {
    service_account_email            = google_service_account.watchdog.email
    min_instance_count               = 0
    max_instance_count               = 1
    available_memory                 = "256M"
    available_cpu                    = "0.1666"
    max_instance_request_concurrency = 1
    timeout_seconds                  = 60
    ingress_settings                 = "ALLOW_ALL"
    all_traffic_on_latest_revision   = true
    environment_variables = {
      GCP_PROJECT         = var.project_id
      WATCH_SUBSCRIPTIONS = jsonencode(keys(local.dead_letter_routes))
      LOG_EXECUTION_ID    = "true"
    }
  }
  depends_on = [google_project_iam_member.watchdog, google_bigquery_dataset_iam_member.watchdog]
}

resource "google_cloud_run_v2_service_iam_member" "watchdog" {
  project    = var.project_id
  location   = var.region
  name       = "check-analytics-delivery"
  role       = "roles/run.invoker"
  member     = "serviceAccount:${local.scheduler_service_account}"
  depends_on = [google_cloudfunctions2_function.watchdog]
}

resource "google_cloud_scheduler_job" "watchdog" {
  project          = var.project_id
  region           = var.region
  name             = "check-analytics-delivery"
  schedule         = "*/15 18-20 * * 1-5"
  time_zone        = "Europe/Berlin"
  attempt_deadline = "90s"
  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions2_function.watchdog.service_config[0].uri
    oidc_token {
      service_account_email = local.scheduler_service_account
      audience              = google_cloudfunctions2_function.watchdog.service_config[0].uri
    }
  }
  retry_config {
    retry_count          = 1
    max_retry_duration   = "60s"
    min_backoff_duration = "10s"
    max_backoff_duration = "30s"
    max_doublings        = 1
  }
  depends_on = [google_cloud_run_v2_service_iam_member.watchdog]
}

resource "google_monitoring_alert_policy" "operational_logs" {
  for_each = {
    critical = {
      filter = "((jsonPayload.event=\"operational_alert\" AND severity>=ERROR) OR (logName=\"projects/${var.project_id}/logs/cocoa-operations\" AND jsonPayload.event=\"run_completed\" AND jsonPayload.outcome=\"failed\" AND NOT jsonPayload.engineering_validation=true) OR (resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"check-analytics-delivery\" AND severity>=ERROR) OR (resource.type=\"cloud_scheduler_job\" AND resource.labels.job_id=\"check-analytics-delivery\" AND severity>=ERROR))"
    }
    warning = {
      filter = "((jsonPayload.event=\"operational_alert\" AND severity=WARNING) OR (resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"stage-analytics-data\" AND jsonPayload.event=\"ingestion_quarantined\"))"
    }
  }
  project               = var.project_id
  display_name          = "Cocoa: operational ${each.key}"
  combiner              = "OR"
  notification_channels = local.operator_channels
  user_labels           = { app = "cocoa-prices", severity = each.key }
  conditions {
    display_name = "Actionable ${each.key} event"
    condition_matched_log {
      filter           = each.value.filter
      label_extractors = { alert_code = "EXTRACT(jsonPayload.code)" }
    }
  }
  alert_strategy {
    notification_rate_limit { period = "300s" }
    auto_close = "1800s"
  }
  documentation {
    mime_type = "text/markdown"
    content   = "Alert code: $${log.extracted_label.alert_code}. Inspect the triggering log and [operations dashboard](https://console.cloud.google.com/monitoring/dashboards/builder/b5f39305-22ae-44ea-983e-ffb23c40de73?project=${var.project_id}). Use release/manifest/run IDs to inspect the accepted release and task states. For platform errors the code can be empty: inspect the source Scheduler/function entry. Acknowledge in Monitoring; snooze during active recovery if needed. Do not delete queues, overwrite data or retrain automatically. Follow docs/operations/actionable-alerts.md. Code setup_test is the owner-requested delivery test, not a real outage."
  }
}
