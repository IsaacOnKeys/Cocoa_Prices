-- Render identifiers with scripts/deploy/render_delivery_sql.py before applying.
-- These permanent entities are created before the daily DML transactions.
CREATE TABLE IF NOT EXISTS `${project}.${delivery}.release_runs` (
  release_id STRING NOT NULL,
  business_date DATE NOT NULL,
  dag_run_id STRING NOT NULL,
  run_started_at TIMESTAMP,
  input_cutoff TIMESTAMP,
  base_release_id STRING,
  code_id STRING NOT NULL,
  source_receipts JSON,
  source_receipts_sha256 STRING,
  baseline_fingerprints JSON,
  status STRING NOT NULL,
  model_id STRING,
  quality_results JSON,
  created_at TIMESTAMP NOT NULL,
  validated_at TIMESTAMP,
  published_at TIMESTAMP,
  last_error STRING
) PARTITION BY business_date CLUSTER BY release_id;

CREATE TABLE IF NOT EXISTS `${project}.${delivery}.active_release` (
  singleton INT64 NOT NULL,
  release_id STRING NOT NULL,
  business_date DATE NOT NULL,
  published_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `${project}.${delivery}.release_events` (
  event_id STRING NOT NULL,
  kind STRING NOT NULL,
  from_release_id STRING NOT NULL,
  to_release_id STRING NOT NULL,
  reason STRING NOT NULL,
  created_at TIMESTAMP NOT NULL
) PARTITION BY DATE(created_at) CLUSTER BY event_id;

CREATE TABLE IF NOT EXISTS `${project}.${delivery}.frozen_inputs` (
  release_id STRING NOT NULL,
  business_date DATE NOT NULL,
  kind STRING NOT NULL,
  feed STRING NOT NULL,
  date DATE,
  price FLOAT64,
  precipitation FLOAT64,
  soil_moisture FLOAT64,
  acquisition_id STRING,
  observation_id STRING,
  producer_acquired_at TIMESTAMP,
  consumer_received_at TIMESTAMP,
  source_published_at TIMESTAMP,
  timestamp_status STRING
) PARTITION BY business_date CLUSTER BY release_id, feed;

CREATE TABLE IF NOT EXISTS `${project}.${delivery}.cocoa` (
  release_id STRING NOT NULL, business_date DATE NOT NULL,
  Date DATE, Euro_Price FLOAT64
) PARTITION BY business_date CLUSTER BY release_id;

CREATE TABLE IF NOT EXISTS `${project}.${delivery}.brent_prices` (
  release_id STRING NOT NULL, business_date DATE NOT NULL,
  date DATE, brent_price_eu FLOAT64
) PARTITION BY business_date CLUSTER BY release_id;

CREATE TABLE IF NOT EXISTS `${project}.${delivery}.precipitation` (
  release_id STRING NOT NULL, business_date DATE NOT NULL,
  date DATE, precipitation FLOAT64, soil_moisture FLOAT64
) PARTITION BY business_date CLUSTER BY release_id;

CREATE TABLE IF NOT EXISTS `${project}.${delivery}.joined_daily_cocoa_oil_weather` (
  release_id STRING NOT NULL, business_date DATE NOT NULL,
  date DATE, cocoa_price FLOAT64, oil_price FLOAT64,
  precipitation FLOAT64, soil_moisture FLOAT64
) PARTITION BY business_date CLUSTER BY release_id;

CREATE TABLE IF NOT EXISTS `${project}.${delivery}.features` (
  release_id STRING NOT NULL, business_date DATE NOT NULL,
  date DATE, y FLOAT64, y_lag1 FLOAT64, y_lag7 FLOAT64, y_lag28 FLOAT64,
  oil_ma7 FLOAT64, precip_ma7 FLOAT64, soil_ma7 FLOAT64,
  dow STRING, mon STRING, target_40 FLOAT64
) PARTITION BY business_date CLUSTER BY release_id;

CREATE TABLE IF NOT EXISTS `${project}.${delivery}.forecast_40d` (
  release_id STRING NOT NULL, business_date DATE NOT NULL,
  forecast_date DATE, forecast_cocoa_price FLOAT64
) PARTITION BY business_date CLUSTER BY release_id;

CREATE TABLE IF NOT EXISTS `${project}.${delivery}.pred_hist_40d` (
  release_id STRING NOT NULL, business_date DATE NOT NULL,
  prediction_date DATE, forecast_date DATE, yhat_40 FLOAT64, actual FLOAT64
) PARTITION BY business_date CLUSTER BY release_id;
