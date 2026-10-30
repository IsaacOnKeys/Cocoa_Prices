locals {
  delivery_source_filter = "resource.type = \"pubsub_subscription\" AND (${join(" OR ", [for name in keys(local.dead_letter_routes) : "resource.labels.subscription_id = \"${name}\""])})"
  delivery_export_filter = "resource.type = \"pubsub_subscription\" AND (${join(" OR ", [for name in local.dead_letter_topics : "resource.labels.subscription_id = \"${name}-sub\""])})"
  operations_run_filter  = "resource.type = \"cloud_run_revision\" AND resource.labels.service_name = monitoring.regex.full_match(\"publish-.*|stage-analytics-data\")"
  operations_vm_filter   = "resource.type = \"gce_instance\" AND resource.labels.instance_id = \"${google_compute_instance.airflow.instance_id}\""
  operations_charts = [
    { title = "Staging/trigger backlog", filter = local.delivery_source_filter, metric = "pubsub.googleapis.com/subscription/num_unacked_messages_by_region", aligner = "ALIGN_MAX" },
    { title = "Oldest staging/trigger message (seconds)", filter = local.delivery_source_filter, metric = "pubsub.googleapis.com/subscription/oldest_unacked_message_age_by_region", aligner = "ALIGN_MAX" },
    { title = "Messages forwarded to recovery", filter = local.delivery_source_filter, metric = "pubsub.googleapis.com/subscription/dead_letter_message_count", aligner = "ALIGN_SUM" },
    { title = "Recovery export backlog", filter = local.delivery_export_filter, metric = "pubsub.googleapis.com/subscription/num_unacked_messages_by_region", aligner = "ALIGN_MAX" },
    { title = "Oldest recovery message (seconds)", filter = local.delivery_export_filter, metric = "pubsub.googleapis.com/subscription/oldest_unacked_message_age_by_region", aligner = "ALIGN_MAX" },
    { title = "Producer/consumer requests per second", filter = local.operations_run_filter, metric = "run.googleapis.com/request_count", aligner = "ALIGN_RATE" },
    { title = "Producer/consumer 5xx per second", filter = "${local.operations_run_filter} AND metric.labels.response_code_class = \"5xx\"", metric = "run.googleapis.com/request_count", aligner = "ALIGN_RATE" },
    { title = "Producer/consumer latency p95", filter = local.operations_run_filter, metric = "run.googleapis.com/request_latencies", aligner = "ALIGN_PERCENTILE_95" },
    { title = "Airflow VM CPU utilization", filter = local.operations_vm_filter, metric = "compute.googleapis.com/instance/cpu/utilization", aligner = "ALIGN_MEAN" },
    { title = "Airflow VM memory used (%)", filter = "${local.operations_vm_filter} AND metric.labels.state = \"used\"", metric = "agent.googleapis.com/memory/percent_used", aligner = "ALIGN_MEAN" }
  ]
}

resource "google_monitoring_alert_policy" "dead_letter_forwarded" {
  project      = var.project_id
  display_name = "Cocoa: message forwarded to dead-letter recovery"
  combiner     = "OR"
  conditions {
    display_name = "Any successful dead-letter forwarding in five minutes"
    condition_threshold {
      filter          = "${local.delivery_source_filter} AND metric.type = \"pubsub.googleapis.com/subscription/dead_letter_message_count\" AND metric.labels.response_code = \"success\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"
      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_SUM"
      }
      trigger {
        count = 1
      }
    }
  }
  documentation {
    mime_type = "text/markdown"
    content   = "Inspect the native Avro recovery export, diagnose the original failure and use the pinned-generation replay procedure in docs/operations/dead-letter-recovery.md. No automatic replay. Notification routing is tracked in checklist item 26; this policy creates console incidents."
  }
  alert_strategy {
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "dead_letter_export_stalled" {
  project      = var.project_id
  display_name = "Cocoa: dead-letter storage export stalled"
  combiner     = "OR"
  conditions {
    display_name = "Recovery backlog older than five minutes for five minutes"
    condition_threshold {
      filter          = "${local.delivery_export_filter} AND metric.type = \"pubsub.googleapis.com/subscription/oldest_unacked_message_age_by_region\""
      comparison      = "COMPARISON_GT"
      threshold_value = 300
      duration        = "300s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
      trigger {
        count = 1
      }
    }
  }
  documentation {
    mime_type = "text/markdown"
    content   = "Check cloudStorageConfig.state, bucket permissions and availability. Do not replace or delete the subscription: seven days of unacknowledged retention protect export failures. Follow docs/operations/dead-letter-recovery.md. Notification routing remains checklist item 26."
  }
  alert_strategy {
    auto_close = "1800s"
  }
}

resource "google_monitoring_dashboard" "dead_letter_recovery" {
  project = var.project_id
  dashboard_json = jsonencode({
    displayName = "Cocoa analytics operations"
    gridLayout = {
      columns = "2"
      widgets = concat([{
        title = "Delivery, source age, coverage and run diagnosis"
        text = {
          format  = "MARKDOWN"
          content = "Latest structured run entries below show task outcomes, source ages/counts, duration and manifest IDs. Failed runs retain the previous accepted release. For authoritative current delivery/quality status, open [BigQuery](https://console.cloud.google.com/bigquery?project=${var.project_id}) and run `SELECT * FROM ${var.project_id}.cocoa_related_studio.operational_status`. No data in a chart is not proof of health; VM metrics are expected only during scheduled uptime. See docs/operations/operational-telemetry.md."
        }
        }, {
        title = "Run outcomes and source coverage"
        logsPanel = {
          resourceNames = ["projects/${var.project_id}"]
          filter        = "logName=\"projects/${var.project_id}/logs/cocoa-operations\" NOT jsonPayload.engineering_validation=true"
        }
        }], [for chart in local.operations_charts : {
        title = chart.title
        xyChart = {
          dataSets = [{
            plotType   = "LINE"
            targetAxis = "Y1"
            timeSeriesQuery = {
              timeSeriesFilter = {
                filter      = "${chart.filter} AND metric.type = \"${chart.metric}\""
                aggregation = { alignmentPeriod = "300s", perSeriesAligner = chart.aligner }
              }
            }
          }]
          timeshiftDuration = "0s"
          yAxis             = { label = chart.title, scale = "LINEAR" }
        }
        }], [{
        title = "Application failures, quarantine and retry evidence"
        logsPanel = {
          resourceNames = ["projects/${var.project_id}"]
          filter        = "resource.type=\"cloud_run_revision\" resource.labels.service_name=~\"^(publish-.*|stage-analytics-data)$\" (severity>=WARNING OR jsonPayload.event=\"ingestion_quarantined\" OR jsonPayload.event=\"ingestion_retry\")"
        }
      }])
    }
  })
}
