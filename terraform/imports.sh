#!/usr/bin/env bash
set -euo pipefail

# One-time, idempotent migration record. Run from terraform/ after setting up
# Application Default Credentials. Existing state entries are skipped.
tf_import() {
  local address="$1"
  local id="$2"
  if terraform state show "$address" >/dev/null 2>&1; then
    printf 'Already imported: %s\n' "$address"
  else
    terraform import "$address" "$id"
  fi
}

project="cocoa-prices-430315"
project_number="494039866722"
region="europe-west3"
zone="europe-west3-c"
airflow_sa="airflow-orchestrator@${project}.iam.gserviceaccount.com"
compute_sa="${project_number}-compute@developer.gserviceaccount.com"
scheduler_sa="vm-scheduler-sa@${project}.iam.gserviceaccount.com"
appspot_sa="${project}@appspot.gserviceaccount.com"

tf_import 'google_service_account.custom["api-fetcher"]' "projects/${project}/serviceAccounts/api-fetcher@${project}.iam.gserviceaccount.com"
tf_import 'google_service_account.custom["airflow-orchestrator"]' "projects/${project}/serviceAccounts/${airflow_sa}"
tf_import 'google_service_account.custom["vm-scheduler-sa"]' "projects/${project}/serviceAccounts/${scheduler_sa}"

tf_import 'google_compute_instance.airflow' "projects/${project}/zones/${zone}/instances/airflow-vm"
for name in allow-airflow-ui-myip allow-iap-8080 allow-iap-ssh allow-ssh-from-myip allow-ssh-myip-v4 allow-ssh-myip-v4-2 allow-ssh-myip-v4b allow-ssh-myip-v4c; do
  tf_import "google_compute_firewall.airflow[\"${name}\"]" "projects/${project}/global/firewalls/${name}"
done

for name in cocoa-schema oil-schema weather-schema; do
  tf_import "google_pubsub_schema.schemas[\"${name}\"]" "projects/${project}/schemas/${name}"
done

for name in cocoa-prices-dead-letter oil-prices-dead-letter weather-dead-letter cocoa-prices-topic oil-prices-topic weather-topic oil-trigger weather-trigger; do
  tf_import "google_pubsub_topic.topics[\"${name}\"]" "projects/${project}/topics/${name}"
done

for name in cocoa-prices-sub oil-prices-sub weather-data-sub; do
  tf_import "google_pubsub_subscription.subscriptions[\"${name}\"]" "projects/${project}/subscriptions/${name}"
done

for name in cocoa-prices-temp-for-bq raw-historic-data; do
  tf_import "google_storage_bucket.application[\"${name}\"]" "${project}/${name}"
done

for name in cocoa_related cocoa_related_studio models stream_staging; do
  tf_import "google_bigquery_dataset.datasets[\"${name}\"]" "projects/${project}/datasets/${name}"
done

for name in cocoa_related models stream_staging; do
  tf_import "google_bigquery_dataset_iam_member.airflow_writer[\"${name}\"]" "projects/${project}/datasets/${name} roles/bigquery.dataEditor serviceAccount:${airflow_sa}"
done

for name in FRED_OIL_API_KEY airflow-fernet-key; do
  tf_import "google_secret_manager_secret.secrets[\"${name}\"]" "projects/${project}/secrets/${name}"
  tf_import "google_secret_manager_secret_iam_member.airflow_accessor[\"${name}\"]" "projects/${project}/secrets/${name} roles/secretmanager.secretAccessor serviceAccount:${airflow_sa}"
done

for name in publish_price publish_fred_data publish_weather_data; do
  tf_import "google_cloudfunctions2_function.functions[\"${name}\"]" "projects/${project}/locations/${region}/functions/${name}"
  run_name="${name//_/-}"
  tf_import "google_cloud_run_v2_service_iam_member.airflow_invoker[\"${name}\"]" "projects/${project}/locations/${region}/services/${run_name} roles/run.invoker serviceAccount:${airflow_sa}"
done

for name in start-airflow-vm stop-airflow-vm; do
  tf_import "google_cloud_scheduler_job.airflow[\"${name}\"]" "projects/${project}/locations/${region}/jobs/${name}"
done

tf_import 'google_storage_bucket_iam_member.application["temp_compute_object_admin"]' "b/cocoa-prices-temp-for-bq roles/storage.objectAdmin serviceAccount:${compute_sa}"
tf_import 'google_storage_bucket_iam_member.application["temp_airflow_object_viewer"]' "b/cocoa-prices-temp-for-bq roles/storage.objectViewer serviceAccount:${airflow_sa}"
tf_import 'google_storage_bucket_iam_member.application["raw_compute_object_creator"]' "b/raw-historic-data roles/storage.objectCreator serviceAccount:${compute_sa}"
tf_import 'google_storage_bucket_iam_member.application["raw_compute_object_viewer"]' "b/raw-historic-data roles/storage.objectViewer serviceAccount:${compute_sa}"

tf_import 'google_project_iam_member.application["compute_artifact_reader"]' "${project} roles/artifactregistry.reader serviceAccount:${compute_sa}"
tf_import 'google_project_iam_member.application["compute_bigquery_editor"]' "${project} roles/bigquery.dataEditor serviceAccount:${compute_sa}"
tf_import 'google_project_iam_member.application["compute_bigquery_viewer"]' "${project} roles/bigquery.dataViewer serviceAccount:${compute_sa}"
tf_import 'google_project_iam_member.application["compute_bigquery_job_user"]' "${project} roles/bigquery.jobUser serviceAccount:${compute_sa}"
tf_import 'google_project_iam_member.application["compute_cloudbuild_builder"]' "${project} roles/cloudbuild.builds.builder serviceAccount:${compute_sa}"
tf_import 'google_project_iam_member.application["compute_cloudfunctions_invoke"]' "${project} roles/cloudfunctions.invoker serviceAccount:${compute_sa}"
tf_import 'google_project_iam_member.application["compute_instance_admin"]' "${project} roles/compute.instanceAdmin serviceAccount:${compute_sa}"
tf_import 'google_project_iam_member.application["compute_dataflow_worker"]' "${project} roles/dataflow.worker serviceAccount:${compute_sa}"
tf_import 'google_project_iam_member.application["compute_pubsub_publisher"]' "${project} roles/pubsub.publisher serviceAccount:${compute_sa}"
tf_import 'google_project_iam_member.application["compute_pubsub_subscriber"]' "${project} roles/pubsub.subscriber serviceAccount:${compute_sa}"
tf_import 'google_project_iam_member.application["compute_secret_accessor"]' "${project} roles/secretmanager.secretAccessor serviceAccount:${compute_sa}"
tf_import 'google_project_iam_member.application["compute_storage_creator"]' "${project} roles/storage.objectCreator serviceAccount:${compute_sa}"
tf_import 'google_project_iam_member.application["compute_storage_viewer"]' "${project} roles/storage.objectViewer serviceAccount:${compute_sa}"

tf_import 'google_project_iam_member.application["airflow_bigquery_viewer"]' "${project} roles/bigquery.dataViewer serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_bigquery_job_user"]' "${project} roles/bigquery.jobUser serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_cloudfunctions_viewer"]' "${project} roles/cloudfunctions.viewer serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_eventarc_viewer"]' "${project} roles/eventarc.viewer serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_logging_writer"]' "${project} roles/logging.logWriter serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_logging_viewer"]' "${project} roles/logging.viewer serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_monitoring_writer"]' "${project} roles/monitoring.metricWriter serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_monitoring_viewer"]' "${project} roles/monitoring.viewer serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_pubsub_editor"]' "${project} roles/pubsub.editor serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_pubsub_publisher"]' "${project} roles/pubsub.publisher serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_pubsub_subscriber"]' "${project} roles/pubsub.subscriber serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_pubsub_viewer"]' "${project} roles/pubsub.viewer serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_run_invoker"]' "${project} roles/run.invoker serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_run_viewer"]' "${project} roles/run.viewer serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_secret_accessor"]' "${project} roles/secretmanager.secretAccessor serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_serviceusage_consumer"]' "${project} roles/serviceusage.serviceUsageConsumer serviceAccount:${airflow_sa}"
tf_import 'google_project_iam_member.application["airflow_storage_viewer"]' "${project} roles/storage.objectViewer serviceAccount:${airflow_sa}"

tf_import 'google_project_iam_member.application["scheduler_instance_admin"]' "${project} roles/compute.instanceAdmin.v1 serviceAccount:${scheduler_sa}"
tf_import 'google_project_iam_member.application["appspot_secret_accessor"]' "${project} roles/secretmanager.secretAccessor serviceAccount:${appspot_sa}"
