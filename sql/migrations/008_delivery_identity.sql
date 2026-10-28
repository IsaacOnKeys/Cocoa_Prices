-- Additive transport/logical identity; do not fabricate identifiers for old rows.
ALTER TABLE `cocoa-prices-430315.stream_staging.cocoa_prices`
  ADD COLUMN IF NOT EXISTS delivery_id STRING,
  ADD COLUMN IF NOT EXISTS pubsub_message_id STRING,
  ADD COLUMN IF NOT EXISTS acquisition_id STRING,
  ADD COLUMN IF NOT EXISTS observation_id STRING;
ALTER TABLE `cocoa-prices-430315.stream_staging.oil_prices`
  ADD COLUMN IF NOT EXISTS delivery_id STRING,
  ADD COLUMN IF NOT EXISTS pubsub_message_id STRING,
  ADD COLUMN IF NOT EXISTS acquisition_id STRING,
  ADD COLUMN IF NOT EXISTS observation_id STRING;
ALTER TABLE `cocoa-prices-430315.stream_staging.precipitation_moisture`
  ADD COLUMN IF NOT EXISTS delivery_id STRING,
  ADD COLUMN IF NOT EXISTS pubsub_message_id STRING,
  ADD COLUMN IF NOT EXISTS acquisition_id STRING,
  ADD COLUMN IF NOT EXISTS observation_id STRING;
