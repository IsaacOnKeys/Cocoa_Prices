locals {
  custom_service_accounts = {
    api-fetcher = {
      display_name = "API Fetcher for Pub/Sub"
    }
    airflow-orchestrator = {
      display_name = "airflow-orchestrator"
    }
    vm-scheduler-sa = {
      display_name = "VM Scheduler Service Account"
    }
  }

  project_iam_members = {
    compute_artifact_reader       = { role = "roles/artifactregistry.reader", member = "serviceAccount:${local.compute_service_account}" }
    compute_bigquery_editor       = { role = "roles/bigquery.dataEditor", member = "serviceAccount:${local.compute_service_account}" }
    compute_bigquery_viewer       = { role = "roles/bigquery.dataViewer", member = "serviceAccount:${local.compute_service_account}" }
    compute_bigquery_job_user     = { role = "roles/bigquery.jobUser", member = "serviceAccount:${local.compute_service_account}" }
    compute_cloudbuild_builder    = { role = "roles/cloudbuild.builds.builder", member = "serviceAccount:${local.compute_service_account}" }
    compute_cloudfunctions_invoke = { role = "roles/cloudfunctions.invoker", member = "serviceAccount:${local.compute_service_account}" }
    compute_instance_admin        = { role = "roles/compute.instanceAdmin", member = "serviceAccount:${local.compute_service_account}" }
    compute_dataflow_worker       = { role = "roles/dataflow.worker", member = "serviceAccount:${local.compute_service_account}" }
    compute_pubsub_publisher      = { role = "roles/pubsub.publisher", member = "serviceAccount:${local.compute_service_account}" }
    compute_pubsub_subscriber     = { role = "roles/pubsub.subscriber", member = "serviceAccount:${local.compute_service_account}" }
    compute_secret_accessor       = { role = "roles/secretmanager.secretAccessor", member = "serviceAccount:${local.compute_service_account}" }
    compute_storage_creator       = { role = "roles/storage.objectCreator", member = "serviceAccount:${local.compute_service_account}" }
    compute_storage_viewer        = { role = "roles/storage.objectViewer", member = "serviceAccount:${local.compute_service_account}" }

    airflow_bigquery_viewer       = { role = "roles/bigquery.dataViewer", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_bigquery_job_user     = { role = "roles/bigquery.jobUser", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_cloudfunctions_viewer = { role = "roles/cloudfunctions.viewer", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_eventarc_viewer       = { role = "roles/eventarc.viewer", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_logging_writer        = { role = "roles/logging.logWriter", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_logging_viewer        = { role = "roles/logging.viewer", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_monitoring_writer     = { role = "roles/monitoring.metricWriter", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_monitoring_viewer     = { role = "roles/monitoring.viewer", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_pubsub_editor         = { role = "roles/pubsub.editor", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_pubsub_publisher      = { role = "roles/pubsub.publisher", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_pubsub_subscriber     = { role = "roles/pubsub.subscriber", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_pubsub_viewer         = { role = "roles/pubsub.viewer", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_run_invoker           = { role = "roles/run.invoker", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_run_viewer            = { role = "roles/run.viewer", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_secret_accessor       = { role = "roles/secretmanager.secretAccessor", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_serviceusage_consumer = { role = "roles/serviceusage.serviceUsageConsumer", member = "serviceAccount:${local.airflow_service_account}" }
    airflow_storage_viewer        = { role = "roles/storage.objectViewer", member = "serviceAccount:${local.airflow_service_account}" }

    scheduler_instance_admin = { role = "roles/compute.instanceAdmin.v1", member = "serviceAccount:${local.scheduler_service_account}" }
    appspot_secret_accessor  = { role = "roles/secretmanager.secretAccessor", member = "serviceAccount:${local.appspot_service_account}" }
  }
}

resource "google_service_account" "custom" {
  for_each = local.custom_service_accounts

  account_id   = each.key
  project      = var.project_id
  display_name = each.value.display_name

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_project_iam_member" "application" {
  for_each = local.project_iam_members

  project = var.project_id
  role    = each.value.role
  member  = each.value.member
}
