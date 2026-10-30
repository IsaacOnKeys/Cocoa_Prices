-- Reconcile independently calculated features and the complete output contracts.
INSERT quality_checks
WITH source_revisions AS (
  SELECT feed,date,
    COALESCE(MAX(IF(kind='stage',price,NULL)),MAX(IF(kind='baseline',price,NULL))) AS price,
    COALESCE(MAX(IF(kind='stage',precipitation,NULL)),MAX(IF(kind='baseline',precipitation,NULL))) AS precipitation,
    COALESCE(MAX(IF(kind='stage',soil_moisture,NULL)),MAX(IF(kind='baseline',soil_moisture,NULL))) AS soil_moisture
  FROM source_input GROUP BY feed,date
), curated_candidates AS (
  SELECT 'cocoa' AS feed,Date AS date,Euro_Price AS price,
    CAST(NULL AS FLOAT64) AS precipitation,CAST(NULL AS FLOAT64) AS soil_moisture FROM candidate_cocoa
  UNION ALL SELECT 'oil',date,brent_price_eu,NULL,NULL FROM candidate_oil
  UNION ALL SELECT 'weather',date,NULL,precipitation,soil_moisture FROM candidate_weather
), source_calendar AS (
  SELECT d AS date,c.price AS cocoa_raw,o.price AS oil_raw,
    NULLIF(w.precipitation,-999.0) AS precipitation_raw,NULLIF(w.soil_moisture,-999.0) AS soil_raw
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2014-01-02',p_business_date)) d
  LEFT JOIN (SELECT * FROM source_revisions WHERE feed='cocoa') c ON d=c.date
  LEFT JOIN (SELECT * FROM source_revisions WHERE feed='oil') o ON d=o.date
  LEFT JOIN (SELECT * FROM source_revisions WHERE feed='weather') w ON d=w.date
), carried_source AS (
  SELECT date,LAST_VALUE(cocoa_raw IGNORE NULLS) OVER dates AS cocoa_price,
    LAST_VALUE(oil_raw IGNORE NULLS) OVER dates AS oil_price,
    LAST_VALUE(precipitation_raw IGNORE NULLS) OVER dates AS precipitation,
    LAST_VALUE(soil_raw IGNORE NULLS) OVER dates AS soil_moisture,
    MAX(IF(cocoa_raw IS NOT NULL,date,NULL)) OVER dates AS cocoa_date,
    MAX(IF(oil_raw IS NOT NULL,date,NULL)) OVER dates AS oil_date,
    MAX(IF(precipitation_raw IS NOT NULL,date,NULL)) OVER dates AS rain_date,
    MAX(IF(soil_raw IS NOT NULL,date,NULL)) OVER dates AS soil_date
  FROM source_calendar WINDOW dates AS (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
), expected_joined AS (
  SELECT date,IF(DATE_DIFF(date,cocoa_date,DAY)<=7,cocoa_price,NULL) AS cocoa_price,
    IF(DATE_DIFF(date,oil_date,DAY)<=14,oil_price,NULL) AS oil_price,
    IF(DATE_DIFF(date,rain_date,DAY)<=7,precipitation,NULL) AS precipitation,
    IF(DATE_DIFF(date,soil_date,DAY)<=7,soil_moisture,NULL) AS soil_moisture
  FROM carried_source
), expected AS (
  SELECT date,cocoa_price AS y,
    LAG(cocoa_price,1) OVER (ORDER BY date) AS lag1,
    LAG(cocoa_price,7) OVER (ORDER BY date) AS lag7,
    LAG(cocoa_price,28) OVER (ORDER BY date) AS lag28,
    AVG(oil_price) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS oil7,
    AVG(precipitation) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS precip7,
    AVG(soil_moisture) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS soil7,
    LEAD(cocoa_price,40) OVER (ORDER BY date) AS label40
  FROM candidate_joined
), feature_comparison AS (
  SELECT COUNTIF(f.date IS NULL OR e.date IS NULL) AS key_errors,
    COUNTIF(f.y IS DISTINCT FROM e.y OR f.y_lag1 IS DISTINCT FROM e.lag1
      OR f.y_lag7 IS DISTINCT FROM e.lag7 OR f.y_lag28 IS DISTINCT FROM e.lag28
      OR f.target_40 IS DISTINCT FROM e.label40) AS value_errors,
    COUNTIF((f.oil_ma7 IS NULL)!=(e.oil7 IS NULL) OR IFNULL(ABS(f.oil_ma7-e.oil7),0)>1e-8
      OR (f.precip_ma7 IS NULL)!=(e.precip7 IS NULL) OR IFNULL(ABS(f.precip_ma7-e.precip7),0)>1e-8
      OR (f.soil_ma7 IS NULL)!=(e.soil7 IS NULL) OR IFNULL(ABS(f.soil_ma7-e.soil7),0)>1e-8) AS rolling_errors,
    COUNTIF(f.dow IS DISTINCT FROM CAST(EXTRACT(DAYOFWEEK FROM e.date) AS STRING)
      OR f.mon IS DISTINCT FROM CAST(EXTRACT(MONTH FROM e.date) AS STRING)) AS calendar_errors
  FROM candidate_features f FULL JOIN expected e USING(date)
)
SELECT 'output','feature_calendar_keys','error',key_errors,'Feature dates exactly match the analytical calendar' FROM feature_comparison
UNION ALL SELECT 'output','curated_source_alignment','error',COUNTIF(c.feed IS NULL OR s.feed IS NULL
  OR c.price IS DISTINCT FROM s.price OR c.precipitation IS DISTINCT FROM s.precipitation
  OR c.soil_moisture IS DISTINCT FROM s.soil_moisture),
  'Curated facts use accepted non-null revisions; a missing revision cannot erase a known fact'
  FROM curated_candidates c FULL JOIN source_revisions s USING(feed,date)
UNION ALL SELECT 'output','joined_source_alignment','error',COUNTIF(j.date IS NULL OR e.date IS NULL
  OR j.cocoa_price IS DISTINCT FROM e.cocoa_price OR j.oil_price IS DISTINCT FROM e.oil_price
  OR j.precipitation IS DISTINCT FROM e.precipitation OR j.soil_moisture IS DISTINCT FROM e.soil_moisture),
  'Joined values reconcile to frozen source revisions and the declared fill rule'
  FROM candidate_joined j FULL JOIN expected_joined e USING(date)
UNION ALL SELECT 'output','feature_lags_and_label','error',value_errors,'Current price, 1/7/28-day lags and 40-day labels match independent expressions' FROM feature_comparison
UNION ALL SELECT 'output','feature_rolling_means','error',rolling_errors,'Seven-row means match within absolute tolerance 1e-8' FROM feature_comparison
UNION ALL SELECT 'output','feature_calendar_categories','error',calendar_errors,'Weekday/month categories use the observation date' FROM feature_comparison
UNION ALL SELECT 'output','joined_calendar_cardinality','error',
  ABS(COUNT(*)-(DATE_DIFF(p_business_date,DATE '2014-01-02',DAY)+1)),
  'Exactly one row per requested analytical calendar date' FROM candidate_joined
UNION ALL SELECT 'output','joined_calendar_keys','error',COUNTIF(date IS NULL OR date<DATE '2014-01-02' OR date>p_business_date)
  +COUNT(*)-COUNT(DISTINCT date),'No missing, duplicate or out-of-bounds joined keys' FROM candidate_joined
UNION ALL SELECT 'output','joined_domains','error',COUNTIF(
  cocoa_price<=0 OR precipitation<0 OR soil_moisture NOT BETWEEN 0 AND 1
  OR IS_NAN(cocoa_price) OR IS_INF(cocoa_price) OR IS_NAN(oil_price) OR IS_INF(oil_price)
  OR IS_NAN(precipitation) OR IS_INF(precipitation) OR IS_NAN(soil_moisture) OR IS_INF(soil_moisture)),
  'Present joined values are finite, in domain and never weather sentinels; leading nulls remain explicit' FROM candidate_joined
UNION ALL SELECT 'output','feature_finiteness','error',COUNTIF(
  IS_NAN(y) OR IS_INF(y) OR IS_NAN(y_lag1) OR IS_INF(y_lag1)
  OR IS_NAN(y_lag7) OR IS_INF(y_lag7) OR IS_NAN(y_lag28) OR IS_INF(y_lag28)
  OR IS_NAN(oil_ma7) OR IS_INF(oil_ma7) OR IS_NAN(precip_ma7) OR IS_INF(precip_ma7)
  OR IS_NAN(soil_ma7) OR IS_INF(soil_ma7) OR IS_NAN(target_40) OR IS_INF(target_40)),
  'Present features/labels are finite; expected warm-up and terminal label nulls are permitted' FROM candidate_features
UNION ALL SELECT 'output','feature_duplicate_dates','error',COUNT(*)-COUNT(DISTINCT date),
  'One feature record per calendar date' FROM candidate_features
UNION ALL SELECT 'output','forecast_count','error',ABS(COUNT(*)-40),
  'The required forecast product contains 40 records' FROM candidate_forecast
UNION ALL SELECT 'output','forecast_unique_dates','error',COUNT(*)-COUNT(DISTINCT forecast_date),
  'One forecast per target date' FROM candidate_forecast
UNION ALL SELECT 'output','forecast_values','error',COUNTIF(forecast_cocoa_price IS NULL
  OR IS_NAN(forecast_cocoa_price) OR IS_INF(forecast_cocoa_price) OR forecast_cocoa_price<=0),
  'Every required forecast is finite and positive' FROM candidate_forecast
UNION ALL SELECT 'output','forecast_calendar','error',COUNTIF(forecast_date IS NULL
  OR forecast_date<=p_business_date OR forecast_date>DATE_ADD(p_business_date,INTERVAL 40 DAY)),
  'Forecast dates span business date +1 through +40; cardinality/uniqueness are checked separately' FROM candidate_forecast
UNION ALL SELECT 'output','historical_score_count','error',
  ABS((SELECT COUNT(*) FROM candidate_predictions)-(SELECT COUNT(*) FROM candidate_features)),
  'Historical scoring covers the same feature calendar'
UNION ALL SELECT 'output','historical_origin_keys','error',COUNTIF(p.prediction_date IS NULL OR f.date IS NULL),
  'Historical origin dates exactly match the feature calendar, even when counts happen to match'
  FROM candidate_predictions p FULL JOIN candidate_features f ON p.prediction_date=f.date
UNION ALL SELECT 'output','historical_score_keys','error',COUNTIF(prediction_date IS NULL
  OR forecast_date IS DISTINCT FROM DATE_ADD(prediction_date,INTERVAL 40 DAY))
  +COUNT(*)-COUNT(DISTINCT prediction_date),'Historical origin keys are unique and preserve the 40-day horizon' FROM candidate_predictions
UNION ALL SELECT 'output','historical_score_values','error',COUNTIF(yhat_40 IS NULL OR IS_NAN(yhat_40) OR IS_INF(yhat_40)),
  'Historical predictions are finite, without asserting out-of-sample accuracy' FROM candidate_predictions
UNION ALL SELECT 'output','historical_actual_alignment','error',COUNTIF(p.actual IS DISTINCT FROM f.y),
  'Actual values join to the correct target date' FROM candidate_predictions p LEFT JOIN candidate_features f ON p.forecast_date=f.date
UNION ALL SELECT 'output','historical_metric_finite','error',IF(metric IS NULL OR IS_NAN(metric) OR IS_INF(metric),1,0),
  'Existing fitted-model historical MAPE is finite; this is not chronological evaluation'
  FROM (SELECT AVG(SAFE_DIVIDE(ABS(actual-yhat_40),actual))*100 AS metric FROM candidate_predictions WHERE actual IS NOT NULL)
UNION ALL SELECT 'output','forecast_scale','warning',COUNTIF(ABS(f.forecast_cocoa_price)>100*ABS(last.y)
  OR ABS(f.forecast_cocoa_price)*100<ABS(last.y)),
  'Record gross 100x scale changes for review, separately from structural forecast errors'
FROM candidate_forecast f CROSS JOIN (SELECT y FROM candidate_features WHERE date=p_business_date) last;

SET quality = (SELECT TO_JSON(STRUCT('output' AS completed_stage,
  ARRAY_AGG(STRUCT(stage,check_name,severity,violations,detail) ORDER BY stage,check_name) AS checks)) FROM quality_checks);
UPDATE `${project}.${delivery}.release_runs` SET quality_results=quality
WHERE release_id=p_release_id AND business_date=p_business_date;
ASSERT (SELECT SUM(IF(severity='error',violations,0)) FROM quality_checks)=0
  AS 'Output quality gate failed; inspect release_runs.quality_results';
