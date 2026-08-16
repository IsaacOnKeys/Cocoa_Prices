locals {
  pubsub_schemas = {
    cocoa-schema   = "${path.module}/../schemas/cocoa_schema.avsc"
    oil-schema     = "${path.module}/../schemas/oil_schema.avsc"
    weather-schema = "${path.module}/../schemas/weather_schema.avsc"
  }

  pubsub_topics = {
    cocoa-prices-dead-letter = null
    oil-prices-dead-letter   = null
    weather-dead-letter      = null
    cocoa-prices-topic       = "cocoa-schema"
    oil-prices-topic         = "oil-schema"
    weather-topic            = "weather-schema"
    oil-trigger              = null
    weather-trigger          = null
  }

  pubsub_subscriptions = {
    cocoa-prices-sub = "cocoa-prices-topic"
    oil-prices-sub   = "oil-prices-topic"
    weather-data-sub = "weather-topic"
  }
}

resource "google_pubsub_schema" "schemas" {
  for_each = local.pubsub_schemas

  name    = each.key
  project = var.project_id
  type    = "AVRO"
  # Pub/Sub stores schema text with LF endings; normalize Windows checkouts to
  # avoid a whitespace-only schema revision.
  definition = replace(file(each.value), "\r\n", "\n")

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_pubsub_topic" "topics" {
  for_each = local.pubsub_topics

  name    = each.key
  project = var.project_id

  dynamic "schema_settings" {
    for_each = each.value == null ? [] : [each.value]
    content {
      schema   = google_pubsub_schema.schemas[schema_settings.value].id
      encoding = "BINARY"
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_pubsub_subscription" "subscriptions" {
  for_each = local.pubsub_subscriptions

  name                       = each.key
  project                    = var.project_id
  topic                      = google_pubsub_topic.topics[each.value].id
  ack_deadline_seconds       = 10
  message_retention_duration = "604800s"
  retain_acked_messages      = false
  enable_message_ordering    = false

  expiration_policy {
    ttl = ""
  }

  lifecycle {
    prevent_destroy = true
  }
}
