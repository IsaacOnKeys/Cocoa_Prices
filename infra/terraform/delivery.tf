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

  project                    = var.project_id
  name                       = "${each.key}-sub"
  topic                      = google_pubsub_topic.topics[each.key].id
  ack_deadline_seconds       = 10
  message_retention_duration = "604800s"
  expiration_policy {
    ttl = ""
  }
  lifecycle {
    prevent_destroy = true
  }
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
