output "airflow_vm" {
  description = "Terraform-managed Airflow Compute Engine instance."
  value = {
    name = google_compute_instance.airflow.name
    zone = google_compute_instance.airflow.zone
  }
}

output "bigquery_datasets" {
  description = "Terraform-managed BigQuery dataset IDs."
  value       = sort([for dataset in google_bigquery_dataset.datasets : dataset.dataset_id])
}

output "cloud_functions" {
  description = "Terraform-managed Gen2 Cloud Function names."
  value       = sort([for function in google_cloudfunctions2_function.functions : function.name])
}

output "pubsub_topics" {
  description = "Terraform-managed application Pub/Sub topic names."
  value       = sort([for topic in google_pubsub_topic.topics : topic.name])
}
