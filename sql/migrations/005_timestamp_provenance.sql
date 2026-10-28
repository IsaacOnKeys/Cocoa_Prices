-- Additive sink migration for checklist item 5. Run before consumer deployment.

-- Existing timestamp/raw columns and historical rows are preserved.

-- NULL timestamp_status identifies rows written before this contract.

ALTER TABLE `cocoa-prices-430315.stream_staging.cocoa_prices`
  ADD COLUMN IF NOT EXISTS producer_acquired_at TIMESTAMP OPTIONS(description='Validated UTC producer acquisition instant; NULL when unknown or invalid'),
  ADD COLUMN IF NOT EXISTS producer_timestamp_raw STRING OPTIONS(description='Unmodified Avro ingestion_time text; NULL if missing'),
  ADD COLUMN IF NOT EXISTS consumer_received_at TIMESTAMP OPTIONS(description='UTC receipt instant of this accepted consumer attempt; never source freshness'),
  ADD COLUMN IF NOT EXISTS source_published_at TIMESTAMP OPTIONS(description='Verified publisher publication instant when supplied; currently unknown for all feeds'),
  ADD COLUMN IF NOT EXISTS timestamp_status STRING OPTIONS(description='valid, legacy_naive_utc, missing, invalid, or future; NULL denotes legacy unassessed rows');

ALTER TABLE `cocoa-prices-430315.stream_staging.oil_prices`
  ADD COLUMN IF NOT EXISTS producer_acquired_at TIMESTAMP OPTIONS(description='Validated UTC producer acquisition instant; NULL when unknown or invalid'),
  ADD COLUMN IF NOT EXISTS producer_timestamp_raw STRING OPTIONS(description='Unmodified Avro ingestion_time text; NULL if missing'),
  ADD COLUMN IF NOT EXISTS consumer_received_at TIMESTAMP OPTIONS(description='UTC receipt instant of this accepted consumer attempt; never source freshness'),
  ADD COLUMN IF NOT EXISTS source_published_at TIMESTAMP OPTIONS(description='Verified publisher publication instant when supplied; currently unknown for all feeds'),
  ADD COLUMN IF NOT EXISTS timestamp_status STRING OPTIONS(description='valid, legacy_naive_utc, missing, invalid, or future; NULL denotes legacy unassessed rows');

ALTER TABLE `cocoa-prices-430315.stream_staging.precipitation_moisture`
  ADD COLUMN IF NOT EXISTS producer_acquired_at TIMESTAMP OPTIONS(description='Validated UTC producer acquisition instant; NULL when unknown or invalid'),
  ADD COLUMN IF NOT EXISTS producer_timestamp_raw STRING OPTIONS(description='Unmodified Avro ingestion_time text; NULL if missing'),
  ADD COLUMN IF NOT EXISTS consumer_received_at TIMESTAMP OPTIONS(description='UTC receipt instant of this accepted consumer attempt; never source freshness'),
  ADD COLUMN IF NOT EXISTS source_published_at TIMESTAMP OPTIONS(description='Verified publisher publication instant when supplied; currently unknown for all feeds'),
  ADD COLUMN IF NOT EXISTS timestamp_status STRING OPTIONS(description='valid, legacy_naive_utc, missing, invalid, or future; NULL denotes legacy unassessed rows');
