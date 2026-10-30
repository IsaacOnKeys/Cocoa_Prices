-- Small per-run records in the existing delivery dataset; no additional service.
CREATE TABLE IF NOT EXISTS `${project}.${delivery}.run_manifests` (
  manifest_id STRING NOT NULL,
  release_id STRING NOT NULL,
  business_date DATE NOT NULL,
  dag_run_id STRING NOT NULL,
  outcome STRING NOT NULL,
  recorded_at TIMESTAMP NOT NULL,
  manifest JSON NOT NULL
)
PARTITION BY business_date
CLUSTER BY release_id
OPTIONS(partition_expiration_days=400, require_partition_filter=TRUE,
  description='Run outcomes, exact inputs, configuration and BigQuery job metadata; one record per summary attempt.');
