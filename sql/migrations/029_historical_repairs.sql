-- Reviewed plans are immutable content-addressed inputs, not direct serving writes.
CREATE TABLE IF NOT EXISTS `${project}.${delivery}.historical_repairs` (
  repair_id STRING NOT NULL,
  plan_json STRING NOT NULL,
  status STRING NOT NULL,
  reason STRING NOT NULL,
  approved_at TIMESTAMP NOT NULL,
  approved_by STRING NOT NULL,
  applied_release_id STRING,
  cancelled_at TIMESTAMP,
  cancelled_by STRING,
  cancellation_reason STRING
) PARTITION BY DATE(approved_at) CLUSTER BY status,repair_id;

ALTER TABLE `${project}.${delivery}.frozen_inputs`
  ADD COLUMN IF NOT EXISTS historical_repair_id STRING;
