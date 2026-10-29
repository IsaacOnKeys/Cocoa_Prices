locals {
  delivery_source_filter = "resource.type = \"pubsub_subscription\" AND (${join(" OR ", [for name in keys(local.dead_letter_routes) : "resource.labels.subscription_id = \"${name}\""])})"
  delivery_export_filter = "resource.type = \"pubsub_subscription\" AND (${join(" OR ", [for name in local.dead_letter_topics : "resource.labels.subscription_id = \"${name}-sub\""])})"
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
    displayName = "Cocoa dead-letter recovery"
    gridLayout = {
      columns = "1"
      widgets = [for chart in [
        { title = "Messages forwarded to recovery", filter = local.delivery_source_filter, metric = "dead_letter_message_count", aligner = "ALIGN_SUM" },
        { title = "Recovery export backlog by region", filter = local.delivery_export_filter, metric = "num_unacked_messages_by_region", aligner = "ALIGN_MAX" },
        { title = "Oldest recovery message age by region (seconds)", filter = local.delivery_export_filter, metric = "oldest_unacked_message_age_by_region", aligner = "ALIGN_MAX" }
        ] : {
        title = chart.title
        xyChart = {
          dataSets = [{
            plotType   = "LINE"
            targetAxis = "Y1"
            timeSeriesQuery = {
              timeSeriesFilter = {
                filter      = "${chart.filter} AND metric.type = \"pubsub.googleapis.com/subscription/${chart.metric}\""
                aggregation = { alignmentPeriod = "300s", perSeriesAligner = chart.aligner }
              }
            }
          }]
          timeshiftDuration = "0s"
          yAxis             = { label = chart.title, scale = "LINEAR" }
        }
      }]
    }
  })
}
