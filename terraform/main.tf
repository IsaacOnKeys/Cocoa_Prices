data "google_compute_network" "default" {
  name    = "default"
  project = var.project_id
}

data "google_compute_subnetwork" "default" {
  name    = "default"
  project = var.project_id
  region  = var.region
}

locals {
  airflow_service_account   = "airflow-orchestrator@${var.project_id}.iam.gserviceaccount.com"
  compute_service_account   = "${var.project_number}-compute@developer.gserviceaccount.com"
  appspot_service_account   = "${var.project_id}@appspot.gserviceaccount.com"
  scheduler_service_account = "vm-scheduler-sa@${var.project_id}.iam.gserviceaccount.com"
}
