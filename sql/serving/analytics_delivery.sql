-- Same freshness columns, now tied to the accepted release rather than arrivals.
CREATE OR REPLACE VIEW `${project}.${studio}.ls_freshness` AS
WITH active AS (
  SELECT r.source_receipts FROM `${project}.${delivery}.active_release` a
  JOIN `${project}.${delivery}.release_runs` r USING(release_id)
  WHERE a.singleton=1
), feeds AS (
  SELECT 'Cocoa Price' AS feed,
    SAFE_CAST(JSON_VALUE(source_receipts,'$.cocoa.acquired_at') AS TIMESTAMP) AS last_ingestion_time,
    'International Cocoa Organization (ICCO)' AS source_name,'https://www.icco.org/statistics/' AS source_url
    FROM active
  UNION ALL
  SELECT 'Oil Price',SAFE_CAST(JSON_VALUE(source_receipts,'$.oil.acquired_at') AS TIMESTAMP),
    'U.S. Energy Information Administration (EIA)','https://www.eia.gov/dnav/pet/hist/RBRTED.htm' FROM active
  UNION ALL
  SELECT 'Precipitation & Soil Moisture',SAFE_CAST(JSON_VALUE(source_receipts,'$.weather.acquired_at') AS TIMESTAMP),
    'NASA POWER','https://power.larc.nasa.gov/api/' FROM active
)
SELECT feed,last_ingestion_time,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),last_ingestion_time,MINUTE) AS minutes_ago,
  ROUND(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),last_ingestion_time,HOUR),1) AS hours_ago,
  source_name,source_url FROM feeds;

CREATE OR REPLACE VIEW `${project}.${studio}.release_status` AS
SELECT a.release_id,a.business_date,a.published_at AS activated_at,r.status,r.code_id,r.model_id,
  r.input_cutoff,r.published_at AS original_publication_time,
  DATE_DIFF(CURRENT_DATE('Europe/Berlin'),a.business_date,DAY) AS release_age_days,
  r.source_receipts,r.quality_results
FROM `${project}.${delivery}.active_release` a
JOIN `${project}.${delivery}.release_runs` r USING(release_id)
WHERE a.singleton=1;
