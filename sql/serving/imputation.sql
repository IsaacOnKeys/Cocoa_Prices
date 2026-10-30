-- Long-form BI source: one row per accepted calendar date and measurement.
-- Retained source/derived artifacts follow the active marker, including rollback.
CREATE OR REPLACE VIEW `${project}.${studio}.ls_series_quality` AS
WITH active AS (
  SELECT a.release_id,a.business_date,
    COALESCE(JSON_VALUE(r.quality_results,'$.imputation_policy.version'),
      'legacy_unbounded_or_unknown') AS policy_version
  FROM `${project}.${delivery}.active_release` a
  JOIN `${project}.${delivery}.release_runs` r USING(release_id)
  WHERE a.singleton=1
), sources AS (
  SELECT s.release_id,s.business_date,s.Date AS date,'cocoa_price' AS measurement,
    s.Euro_Price AS value,TRUE AS row_present
  FROM `${project}.${delivery}.cocoa` s JOIN active a USING(release_id,business_date)
  UNION ALL
  SELECT s.release_id,s.business_date,s.date,'oil_price',s.brent_price_eu,TRUE
  FROM `${project}.${delivery}.brent_prices` s JOIN active a USING(release_id,business_date)
  UNION ALL
  SELECT s.release_id,s.business_date,s.date,m.measurement,NULLIF(m.value,-999.0),TRUE
  FROM `${project}.${delivery}.precipitation` s JOIN active a USING(release_id,business_date)
  CROSS JOIN UNNEST([STRUCT('precipitation' AS measurement,s.precipitation AS value),
    STRUCT('soil_moisture',s.soil_moisture)]) m
), revisions AS (
  SELECT f.release_id,f.business_date,f.date,m.measurement,m.value,
    f.observation_id,f.acquisition_id,TRUE AS row_present
  FROM `${project}.${delivery}.frozen_inputs` f JOIN active a USING(release_id,business_date)
  CROSS JOIN UNNEST([
    STRUCT('cocoa_price' AS measurement,IF(f.feed='cocoa',f.price,NULL) AS value),
    STRUCT('oil_price',IF(f.feed='oil',f.price,NULL)),
    STRUCT('precipitation',f.precipitation),STRUCT('soil_moisture',f.soil_moisture)]) m
  WHERE f.kind='stage' AND ((f.feed='cocoa' AND m.measurement='cocoa_price')
    OR (f.feed='oil' AND m.measurement='oil_price')
    OR (f.feed='weather' AND m.measurement IN ('precipitation','soil_moisture')))
), calendar AS (
  SELECT a.*,j.date,m.measurement,m.published_value,m.carry_limit_days,
    s.value AS source_value,IFNULL(s.row_present,FALSE) AS source_row_present,
    IF(r.row_present,r.value IS NULL,NULL) AS latest_revision_missing,
    IFNULL(r.row_present AND r.value IS NULL AND s.value IS NOT NULL,FALSE) AS preserved_prior_value,
    r.observation_id AS latest_observation_id,r.acquisition_id AS latest_acquisition_id
  FROM `${project}.${delivery}.joined_daily_cocoa_oil_weather` j
  JOIN active a USING(release_id,business_date)
  CROSS JOIN UNNEST([
    STRUCT('cocoa_price' AS measurement,j.cocoa_price AS published_value,7 AS carry_limit_days),
    STRUCT('oil_price',j.oil_price,14),STRUCT('precipitation',j.precipitation,7),
    STRUCT('soil_moisture',j.soil_moisture,7)]) m
  LEFT JOIN sources s ON s.release_id=j.release_id AND s.business_date=j.business_date
    AND s.date=j.date AND s.measurement=m.measurement
  LEFT JOIN revisions r ON r.release_id=j.release_id AND r.business_date=j.business_date
    AND r.date=j.date AND r.measurement=m.measurement
), origins AS (
  SELECT *,MAX(IF(source_value IS NOT NULL,date,NULL)) OVER dates AS original_value_date,
    LAST_VALUE(source_value IGNORE NULLS) OVER dates AS original_value
  FROM calendar WINDOW dates AS (PARTITION BY release_id,measurement ORDER BY date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
), aged AS (
  SELECT *,DATE_DIFF(date,original_value_date,DAY) AS age_days FROM origins
)
SELECT release_id,business_date,date,measurement,policy_version,published_value,
  original_value_date,age_days,carry_limit_days,
  date=business_date AS is_latest,
  IF(date=business_date,DATE_DIFF(CURRENT_DATE('Europe/Berlin'),original_value_date,DAY),NULL)
    AS latest_measurement_age_days,
  IF(date=business_date,original_value_date IS NULL
    OR DATE_DIFF(CURRENT_DATE('Europe/Berlin'),original_value_date,DAY)>carry_limit_days,NULL)
    AS latest_measurement_overdue,
  published_value IS NOT NULL AND original_value_date<date AS is_imputed,
  source_row_present,COALESCE(latest_revision_missing,source_row_present AND source_value IS NULL) AS source_missing,
  CASE WHEN original_value_date IS NULL THEN 'unavailable'
    WHEN age_days>carry_limit_days THEN 'expired'
    WHEN published_value IS NULL THEN 'missing'
    WHEN published_value IS DISTINCT FROM original_value THEN 'inconsistent'
    WHEN age_days=0 THEN 'observed' ELSE 'carried' END AS quality_status,
  IF(age_days<=carry_limit_days AND published_value IS NOT DISTINCT FROM original_value,
    published_value,NULL) AS usable_value,
  latest_revision_missing,preserved_prior_value,latest_observation_id,latest_acquisition_id
FROM aged;
