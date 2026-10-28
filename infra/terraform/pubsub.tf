locals {
  pubsub_schemas = {
    cocoa-schema   = "${path.module}/../../schemas/cocoa_schema.avsc"
    oil-schema     = "${path.module}/../../schemas/oil_schema.avsc"
    weather-schema = "${path.module}/../../schemas/weather_schema.avsc"
  }

  pubsub_topics = {
    cocoa-prices-dead-letter    = null
    oil-prices-dead-letter      = null
    weather-dead-letter         = null
    eia-trigger-dead-letter     = null
    weather-trigger-dead-letter = null
    cocoa-prices-topic          = "cocoa-schema"
    oil-prices-topic            = "oil-schema"
    weather-topic               = "weather-schema"
    eia-oil-trigger             = null
    eia-oil-validation          = "oil-schema"
    weather-trigger             = null
  }

  pubsub_subscriptions = {
    cocoa-prices-sub       = "cocoa-prices-topic"
    oil-prices-sub         = "oil-prices-topic"
    eia-oil-validation-sub = "eia-oil-validation"
    weather-data-sub       = "weather-topic"
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
  ack_deadline_seconds       = var.daily_ingestion_push_enabled && contains(keys(local.daily_ingestion_feeds), each.key) ? 60 : 10
  message_retention_duration = "604800s"
  retain_acked_messages      = false
  enable_message_ordering    = false

  dynamic "push_config" {
    for_each = var.daily_ingestion_push_enabled && contains(keys(local.daily_ingestion_feeds), each.key) ? [1] : []
    content {
      push_endpoint = google_cloudfunctions2_function.ingestion.service_config[0].uri
      oidc_token {
        service_account_email = google_service_account.ingestion_push.email
        audience              = google_cloudfunctions2_function.ingestion.service_config[0].uri
      }
    }
  }

  dynamic "dead_letter_policy" {
    for_each = contains(keys(local.canonical_dead_letters), each.key) ? [local.canonical_dead_letters[each.key]] : []
    content {
      dead_letter_topic     = google_pubsub_topic.topics[dead_letter_policy.value].id
      max_delivery_attempts = 5
    }
  }

  dynamic "retry_policy" {
    for_each = contains(keys(local.canonical_dead_letters), each.key) ? [1] : []
    content {
      minimum_backoff = "10s"
      maximum_backoff = "60s"
    }
  }

  depends_on = [
    google_cloud_run_v2_service_iam_member.ingestion_push_invoker,
    google_service_account_iam_member.ingestion_push_tokens,
    google_bigquery_dataset_iam_member.ingestion_staging_writer,
    google_pubsub_subscription.dead_letters,
  ]

  expiration_policy {
    ttl = ""
  }

  lifecycle {
    prevent_destroy = true
  }
}
