locals {
  canonical_dead_letters = {
    cocoa-prices-sub = "cocoa-prices-dead-letter"
    oil-prices-sub   = "oil-prices-dead-letter"
    weather-data-sub = "weather-dead-letter"
  }
  # Cloud Functions owns the triggers and their authenticated push routing.
  # Reconcile these transport names after any trigger replacement.
  source_trigger_subscriptions = {
    oil = {
      subscription = "eventarc-europe-west3-publish-eia-data-693553-sub-409"
      topic        = "eia-oil-trigger"
      dead_letter  = "eia-trigger-dead-letter"
    }
    weather = {
      subscription = "eventarc-europe-west3-publish-weather-data-060602-sub-655"
      topic        = "weather-trigger"
      dead_letter  = "weather-trigger-dead-letter"
    }
  }
  dead_letter_routes = merge(local.canonical_dead_letters, {
    for config in values(local.source_trigger_subscriptions) : config.subscription => config.dead_letter
  })
  dead_letter_topics   = toset(values(local.dead_letter_routes))
  pubsub_service_agent = "serviceAccount:service-${var.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

import {
  for_each = local.source_trigger_subscriptions
  to       = google_pubsub_subscription.source_triggers[each.key]
  id       = "projects/${var.project_id}/subscriptions/${each.value.subscription}"
}

resource "google_pubsub_subscription" "source_triggers" {
  for_each = local.source_trigger_subscriptions

  project                    = var.project_id
  name                       = each.value.subscription
  topic                      = google_pubsub_topic.topics[each.value.topic].id
  ack_deadline_seconds       = 600
  message_retention_duration = "86400s"
  retain_acked_messages      = false

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "60s"
  }
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.topics[each.value.dead_letter].id
    max_delivery_attempts = 5
  }
  expiration_policy {
    ttl = ""
  }

  depends_on = [google_pubsub_subscription.dead_letters]

  lifecycle {
    prevent_destroy = true
    # Eventarc retains ownership of endpoint, OIDC and its management labels.
    ignore_changes = [push_config, labels]
  }
}

resource "google_pubsub_subscription" "dead_letters" {
  for_each = local.dead_letter_topics

  project = var.project_id
  name    = "${each.key}-sub"
  topic   = google_pubsub_topic.topics[each.key].id
  # Cloud Storage export normalizes the transport ACK deadline to max_duration.
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  cloud_storage_config {
    bucket          = google_storage_bucket.dead_letter_recovery.name
    filename_prefix = "messages/v1/${each.key}/"
    filename_suffix = ".avro"
    max_duration    = "60s"
    max_bytes       = 1048576
    avro_config {
      write_metadata   = true
      use_topic_schema = false
    }
  }
  expiration_policy {
    ttl = ""
  }
  lifecycle {
    prevent_destroy = true
  }
  depends_on = [google_storage_bucket_iam_member.dead_letter_exports]
}

resource "google_storage_bucket" "dead_letter_recovery" {
  name                        = "${var.project_id}-dead-letters"
  project                     = var.project_id
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  labels                      = { app = "cocoa-prices", component = "dead-letter-recovery" }
  versioning {
    enabled = true
  }
  soft_delete_policy {
    retention_duration_seconds = 0
  }
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age            = 400
      with_state     = "LIVE"
      matches_prefix = ["messages/v1/"]
    }
  }
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age                        = 400
      with_state                 = "ARCHIVED"
      days_since_noncurrent_time = 30
      matches_prefix             = ["messages/v1/"]
    }
  }
  lifecycle {
    prevent_destroy = true
  }
}

# Native export requires these bucket-level grants without object conditions.
# Keep operator replay ledgers in the separate raw bucket, outside these grants.
resource "google_storage_bucket_iam_member" "dead_letter_exports" {
  for_each = {
    creator  = "roles/storage.objectCreator"
    metadata = "roles/storage.legacyBucketReader"
  }
  bucket = google_storage_bucket.dead_letter_recovery.name
  role   = each.value
  member = local.pubsub_service_agent
}

resource "google_pubsub_topic_iam_member" "dead_letter_publisher" {
  for_each = local.dead_letter_topics

  project = var.project_id
  topic   = google_pubsub_topic.topics[each.key].name
  role    = "roles/pubsub.publisher"
  member  = local.pubsub_service_agent
  # A forwarding destination must have a durable subscriber before it is usable.
  depends_on = [google_pubsub_subscription.dead_letters]
}

resource "google_pubsub_subscription_iam_member" "dead_letter_forwarder" {
  for_each = local.dead_letter_routes

  project      = var.project_id
  subscription = each.key
  role         = "roles/pubsub.subscriber"
  member       = local.pubsub_service_agent
  depends_on = [
    google_pubsub_subscription.subscriptions,
    google_pubsub_subscription.source_triggers,
    google_pubsub_topic_iam_member.dead_letter_publisher,
  ]
}
