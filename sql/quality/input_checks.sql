-- Source inventory comes from the exact generation/hash accepted by each gate.
CREATE TEMP TABLE expected_sources AS
SELECT 'cocoa' AS feed,SAFE_CAST(JSON_VALUE(p_sources,'$.cocoa.window_start') AS DATE) AS window_start,
  SAFE_CAST(JSON_VALUE(p_sources,'$.cocoa.window_end') AS DATE) AS window_end,
  SAFE_CAST(JSON_VALUE(p_sources,'$.cocoa.observations') AS INT64) AS observations,
  SAFE_CAST(JSON_VALUE(p_sources,'$.cocoa.acquired_at') AS TIMESTAMP) AS acquired_at,
  JSON_QUERY(p_sources,'$.cocoa.observation_manifest') AS inventory
UNION ALL SELECT 'oil',SAFE_CAST(JSON_VALUE(p_sources,'$.oil.window_start') AS DATE),
  SAFE_CAST(JSON_VALUE(p_sources,'$.oil.window_end') AS DATE),
  SAFE_CAST(JSON_VALUE(p_sources,'$.oil.observations') AS INT64),
  SAFE_CAST(JSON_VALUE(p_sources,'$.oil.acquired_at') AS TIMESTAMP),
  JSON_QUERY(p_sources,'$.oil.observation_manifest')
UNION ALL SELECT 'weather',SAFE_CAST(JSON_VALUE(p_sources,'$.weather.window_start') AS DATE),
  SAFE_CAST(JSON_VALUE(p_sources,'$.weather.window_end') AS DATE),
  SAFE_CAST(JSON_VALUE(p_sources,'$.weather.observations') AS INT64),
  SAFE_CAST(JSON_VALUE(p_sources,'$.weather.acquired_at') AS TIMESTAMP),
  JSON_QUERY(p_sources,'$.weather.observation_manifest');

CREATE TEMP TABLE expected_observations AS
SELECT feed,SAFE_CAST(JSON_VALUE(o,'$.date') AS DATE) AS date,
  JSON_VALUE(o,'$.observation_id') AS observation_id,
  JSON_QUERY(o,'$.missing_measurements') AS missing
FROM expected_sources,UNNEST(JSON_QUERY_ARRAY(inventory)) o;

CREATE TEMP TABLE quality_checks AS
SELECT 'input' AS stage,'source_receipt_contract' AS check_name,'error' AS severity,
  COUNTIF(window_start IS NULL OR window_end IS NULL OR acquired_at IS NULL
    OR observations IS NULL OR observations NOT BETWEEN 0 AND 90
    OR JSON_TYPE(inventory) IS DISTINCT FROM 'array'
    OR ARRAY_LENGTH(JSON_QUERY_ARRAY(inventory)) IS DISTINCT FROM observations
    OR DATE_DIFF(window_end,window_start,DAY) NOT BETWEEN 30 AND 89
    OR window_end IS DISTINCT FROM DATE(acquired_at) OR window_end>p_business_date) AS violations,
  'Require the bounded acquired window and complete accepted inventory' AS detail
FROM expected_sources
UNION ALL
SELECT 'input','frozen_row_shape','error',COUNTIF(feed NOT IN ('cocoa','oil','weather')
  OR kind NOT IN ('baseline','stage') OR (feed='weather' AND price IS NOT NULL)
  OR (feed IN ('cocoa','oil') AND (precipitation IS NOT NULL OR soil_moisture IS NOT NULL))),
  'Frozen source/kind and measurement columns match their feed' FROM source_input
UNION ALL
SELECT 'input','frozen_dates','error',COUNTIF(date IS NULL OR date>p_business_date),
  'Frozen observation dates must be valid and no later than the business date' FROM source_input
UNION ALL
SELECT 'input','frozen_source_keys','error',COUNT(*),'One business revision per source/date/kind'
FROM (SELECT kind,feed,date FROM source_input GROUP BY kind,feed,date HAVING COUNT(*)!=1)
UNION ALL
SELECT 'input','source_manifest_keys','error',COUNT(*),'Accepted source revisions have unique dates and valid identities'
FROM (SELECT feed,date FROM expected_observations GROUP BY feed,date HAVING COUNT(*)!=1)
UNION ALL
SELECT 'input','source_manifest_shape','error',COUNTIF(date IS NULL OR observation_id IS NULL
  OR NOT REGEXP_CONTAINS(observation_id,r'^[a-f0-9]{64}$') OR JSON_TYPE(missing) IS DISTINCT FROM 'array'),
  'Every accepted observation has a date, revision identity and explicit missingness' FROM expected_observations
UNION ALL
SELECT 'input','source_window_reconciliation','error',COUNTIF(s.feed IS NULL OR e.feed IS NULL),
  'No missing or unexpected current-acquisition source revisions'
FROM (SELECT * FROM source_input WHERE kind='stage') s
FULL JOIN expected_observations e ON s.feed=e.feed AND s.date=e.date AND s.observation_id=e.observation_id
UNION ALL
SELECT 'input','staged_window_bounds','error',COUNTIF(s.date<e.window_start OR s.date>e.window_end),
  'Every current observation lies within its acquired source window'
FROM source_input s JOIN expected_sources e USING(feed) WHERE s.kind='stage'
UNION ALL
SELECT 'input','source_missingness','error',COUNTIF(
  (s.feed='cocoa' AND (s.price IS NULL)!=EXISTS(SELECT 1 FROM UNNEST(JSON_VALUE_ARRAY(e.missing)) m WHERE m='cocoa_price'))
  OR (s.feed='oil' AND (s.price IS NULL)!=EXISTS(SELECT 1 FROM UNNEST(JSON_VALUE_ARRAY(e.missing)) m WHERE m='oil_price'))
  OR (s.feed='weather' AND ((s.precipitation IS NULL)!=EXISTS(SELECT 1 FROM UNNEST(JSON_VALUE_ARRAY(e.missing)) m WHERE m='precipitation')
    OR (s.soil_moisture IS NULL)!=EXISTS(SELECT 1 FROM UNNEST(JSON_VALUE_ARRAY(e.missing)) m WHERE m='soil_moisture')))),
  'Source-explicit nulls agree with the accepted receipt'
FROM source_input s JOIN expected_observations e USING(feed,date,observation_id) WHERE s.kind='stage'
UNION ALL
SELECT 'input','frozen_provenance','error',COUNTIF(timestamp_status IS DISTINCT FROM 'valid'
  OR producer_acquired_at IS NULL OR producer_acquired_at<p_run_started_at OR producer_acquired_at>input_cutoff
  OR consumer_received_at IS NULL OR consumer_received_at>input_cutoff OR source_published_at IS NULL),
  'Current rows retain valid acquisition, receipt and transport provenance'
FROM source_input WHERE kind='stage'
UNION ALL
SELECT 'input','finite_measurements','error',COUNTIF(IS_NAN(price) OR IS_INF(price)
  OR IS_NAN(precipitation) OR IS_INF(precipitation) OR IS_NAN(soil_moisture) OR IS_INF(soil_moisture)),
  'All present source measurements are finite' FROM source_input
UNION ALL
SELECT 'input','measurement_domains','error',COUNTIF((feed='cocoa' AND price<=0)
  OR (feed='weather' AND ((precipitation<0 AND NOT(kind='baseline' AND precipitation=-999.0))
    OR ((soil_moisture<0 OR soil_moisture>1) AND NOT(kind='baseline' AND soil_moisture=-999.0))))),
  'Cocoa > 0, precipitation >= 0, wetness in [0,1]; legacy weather -999 is missing, never a value'
FROM source_input
UNION ALL
SELECT 'input','source_age_'||feed||'_'||measurement,'error',
  IF(latest IS NULL OR DATE_DIFF(p_business_date,latest,DAY) NOT BETWEEN 0 AND maximum_age,1,0),
  TO_JSON_STRING(STRUCT(latest AS latest_valid_observation,maximum_age AS maximum_age_days))
FROM (
  SELECT 'cocoa' AS feed,'cocoa_price' AS measurement,MAX(IF(price IS NOT NULL,date,NULL)) AS latest,7 AS maximum_age FROM source_input WHERE kind='stage' AND feed='cocoa'
  UNION ALL SELECT 'oil','oil_price',MAX(IF(price IS NOT NULL,date,NULL)),14 FROM source_input WHERE kind='stage' AND feed='oil'
  UNION ALL SELECT 'weather','precipitation',MAX(IF(precipitation IS NOT NULL,date,NULL)),7 FROM source_input WHERE kind='stage' AND feed='weather'
  UNION ALL SELECT 'weather','soil_moisture',MAX(IF(soil_moisture IS NOT NULL,date,NULL)),7 FROM source_input WHERE kind='stage' AND feed='weather'
)
UNION ALL
SELECT 'input','price_revision_scale','warning',COUNTIF(ABS(s.price)>100*ABS(b.price)
  OR ABS(s.price)*100<ABS(b.price)),
  'Flag gross 100x revision scale changes for review; finite/domain and source gates remain mandatory'
FROM source_input s JOIN source_input b USING(feed,date)
WHERE s.kind='stage' AND b.kind='baseline' AND s.feed IN ('cocoa','oil') AND ABS(b.price)>1e-6;

SET quality = (SELECT TO_JSON(STRUCT('input' AS completed_stage,
  ARRAY_AGG(STRUCT(stage,check_name,severity,violations,detail) ORDER BY check_name) AS checks)) FROM quality_checks);
UPDATE `${project}.${delivery}.release_runs` SET quality_results=quality
WHERE release_id=p_release_id AND business_date=p_business_date;
ASSERT (SELECT SUM(IF(severity='error',violations,0)) FROM quality_checks)=0
  AS 'Input quality gate failed; inspect release_runs.quality_results';
