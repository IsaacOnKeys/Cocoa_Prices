CREATE OR REPLACE VIEW `${project}.${studio}.operational_status` AS
WITH latest_run AS (
  SELECT manifest_id,dag_run_id,business_date,outcome,recorded_at,manifest
  FROM `${project}.${delivery}.run_manifests`
  WHERE business_date BETWEEN DATE_SUB(CURRENT_DATE('Europe/Berlin'),INTERVAL 7 DAY)
    AND CURRENT_DATE('Europe/Berlin')
    AND JSON_VALUE(manifest,'$.run_scope.engineering_validation') IS DISTINCT FROM 'true'
  ORDER BY recorded_at DESC LIMIT 1
)
SELECT p.release_id,p.accepted_business_date,p.original_publication_time,p.delivery_status,p.status_message,
  r.input_cutoff,r.model_id,r.quality_results,
  JSON_QUERY(r.source_receipts,'$.cocoa.source_ages') AS cocoa_age_at_release,
  JSON_QUERY(r.source_receipts,'$.oil.source_ages') AS oil_age_at_release,
  JSON_QUERY(r.source_receipts,'$.weather.source_ages') AS weather_age_at_release,
  SAFE_CAST(JSON_VALUE(r.source_receipts,'$.cocoa.observations') AS INT64) AS cocoa_observations,
  SAFE_CAST(JSON_VALUE(r.source_receipts,'$.oil.observations') AS INT64) AS oil_observations,
  SAFE_CAST(JSON_VALUE(r.source_receipts,'$.weather.observations') AS INT64) AS weather_observations,
  m.manifest_id AS latest_manifest_id,m.dag_run_id AS latest_run_id,m.outcome AS latest_run_outcome,
  m.recorded_at AS latest_run_recorded_at,JSON_QUERY(m.manifest,'$.task_states') AS latest_task_states,
  SAFE_CAST(JSON_VALUE(m.manifest,'$.run_started_at') AS TIMESTAMP) AS latest_run_started_at,
  TIMESTAMP_DIFF(m.recorded_at,SAFE_CAST(JSON_VALUE(m.manifest,'$.run_started_at') AS TIMESTAMP),SECOND) AS processing_duration_seconds,
  m.business_date AS latest_run_business_date
FROM `${project}.${studio}.product_delivery_status` p
LEFT JOIN `${project}.${delivery}.release_runs` r ON r.release_id=p.release_id
LEFT JOIN latest_run m ON TRUE WHERE p.product='core_analytics';
