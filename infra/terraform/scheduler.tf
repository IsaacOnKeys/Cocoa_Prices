locals {
  airflow_scheduler_jobs = {
    start-airflow-vm = {
      schedule = "30 18 * * *"
      action   = "start"
    }
    stop-airflow-vm = {
      schedule = "30 20 * * *"
      action   = "stop"
    }
  }
}

resource "google_cloud_scheduler_job" "airflow" {
  for_each = local.airflow_scheduler_jobs

  name             = each.key
  project          = var.project_id
  region           = var.region
  schedule         = each.value.schedule
  time_zone        = "Europe/Berlin"
  attempt_deadline = "180s"
  paused           = false

  http_target {
    http_method = "POST"
    uri         = "https://compute.googleapis.com/compute/v1/projects/${var.project_id}/zones/${var.zone}/instances/airflow-vm/${each.value.action}"

    oauth_token {
      service_account_email = local.scheduler_service_account
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  retry_config {
    retry_count          = 0
    max_retry_duration   = "0s"
    min_backoff_duration = "5s"
    max_backoff_duration = "3600s"
    max_doublings        = 5
  }

  lifecycle {
    prevent_destroy = true
  }
}
