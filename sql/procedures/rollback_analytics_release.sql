-- Operator-only restoration from retained accepted artifacts, with a CAS marker.
CREATE OR REPLACE PROCEDURE `${project}.${delivery}.rollback_release`(
  p_target STRING,p_expected_current STRING,p_event_id STRING,p_reason STRING
)
BEGIN
  DECLARE target_date DATE;
  DECLARE target_fingerprints JSON;
  DECLARE current_fingerprints JSON;
  DECLARE in_transaction BOOL DEFAULT FALSE;
  BEGIN
  ASSERT REGEXP_CONTAINS(p_target,r'^[a-f0-9]{64}$')
    AND REGEXP_CONTAINS(p_expected_current,r'^[a-f0-9]{64}$')
    AND REGEXP_CONTAINS(p_event_id,r'^[a-f0-9]{64}$')
    AND LENGTH(p_reason) BETWEEN 1 AND 1024 AS 'Invalid explicit rollback request';
  ASSERT (SELECT COUNT(*) FROM `${project}.${delivery}.release_events` WHERE event_id=p_event_id)<=1
    AS 'Duplicate recovery event identity';
  IF EXISTS (SELECT 1 FROM `${project}.${delivery}.release_events` WHERE event_id=p_event_id) THEN
    ASSERT (SELECT COUNT(*) FROM `${project}.${delivery}.release_events` WHERE event_id=p_event_id
      AND kind='rollback' AND from_release_id=p_expected_current AND to_release_id=p_target AND reason=p_reason)=1
      AS 'Recovery event identity was already used for a different request';
  ELSE
    BEGIN TRANSACTION;
    SET in_transaction = TRUE;
    UPDATE `${project}.${delivery}.active_release` SET release_id=release_id WHERE singleton=1;
    ASSERT (SELECT COUNT(*) FROM `${project}.${delivery}.active_release`)=1
      AND (SELECT release_id FROM `${project}.${delivery}.active_release` WHERE singleton=1)=p_expected_current
      AND p_target!=p_expected_current AS 'Current release changed; reconcile before rollback';
    ASSERT (SELECT COUNT(*) FROM `${project}.${delivery}.release_runs`
      WHERE release_id=p_target AND status IN ('published','legacy_verified'))=1
      AS 'Rollback target is not a retained accepted release';
    SET target_date = (SELECT business_date FROM `${project}.${delivery}.release_runs` WHERE release_id=p_target);
    SET target_fingerprints = (SELECT JSON_QUERY(quality_results,'$.artifacts') FROM `${project}.${delivery}.release_runs` WHERE release_id=p_target);
    SET current_fingerprints = (SELECT JSON_QUERY(quality_results,'$.artifacts') FROM `${project}.${delivery}.release_runs` WHERE release_id=p_expected_current);

    CREATE TEMP TABLE before_restore AS
    SELECT 'cocoa' AS name,COUNT(*) AS row_count,COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') AS fingerprint FROM `${project}.${curated}.cocoa` t
    UNION ALL SELECT 'brent_prices',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.brent_prices` t
    UNION ALL SELECT 'precipitation',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.precipitation` t
    UNION ALL SELECT 'joined_daily_cocoa_oil_weather',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.joined_daily_cocoa_oil_weather` t
    UNION ALL SELECT 'forecast_40d',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.forecast_40d` t
    UNION ALL SELECT 'pred_hist_40d',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.pred_hist_40d` t;
    ASSERT (SELECT COUNT(*) FROM before_restore c JOIN UNNEST(JSON_QUERY_ARRAY(current_fingerprints)) b
      ON c.name=JSON_VALUE(b,'$.name') AND c.row_count=SAFE_CAST(JSON_VALUE(b,'$.row_count') AS INT64)
      AND c.fingerprint=JSON_VALUE(b,'$.fingerprint'))=6
      AS 'Current serving data changed outside the release workflow';

    CREATE TEMP TABLE restore_cocoa AS SELECT Date,Euro_Price FROM `${project}.${delivery}.cocoa` WHERE release_id=p_target AND business_date=target_date;
    CREATE TEMP TABLE restore_oil AS SELECT date,brent_price_eu FROM `${project}.${delivery}.brent_prices` WHERE release_id=p_target AND business_date=target_date;
    CREATE TEMP TABLE restore_weather AS SELECT date,precipitation,soil_moisture FROM `${project}.${delivery}.precipitation` WHERE release_id=p_target AND business_date=target_date;
    CREATE TEMP TABLE restore_joined AS SELECT date,cocoa_price,oil_price,precipitation,soil_moisture FROM `${project}.${delivery}.joined_daily_cocoa_oil_weather` WHERE release_id=p_target AND business_date=target_date;
    CREATE TEMP TABLE restore_forecast AS SELECT forecast_date,forecast_cocoa_price FROM `${project}.${delivery}.forecast_40d` WHERE release_id=p_target AND business_date=target_date;
    CREATE TEMP TABLE restore_predictions AS SELECT prediction_date,forecast_date,yhat_40,actual FROM `${project}.${delivery}.pred_hist_40d` WHERE release_id=p_target AND business_date=target_date;
    CREATE TEMP TABLE restore_fingerprints AS
    SELECT 'cocoa' AS name,COUNT(*) AS row_count,COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') AS fingerprint FROM restore_cocoa t
    UNION ALL SELECT 'brent_prices',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM restore_oil t
    UNION ALL SELECT 'precipitation',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM restore_weather t
    UNION ALL SELECT 'joined_daily_cocoa_oil_weather',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM restore_joined t
    UNION ALL SELECT 'forecast_40d',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM restore_forecast t
    UNION ALL SELECT 'pred_hist_40d',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM restore_predictions t;
    ASSERT (SELECT COUNT(*) FROM restore_fingerprints c JOIN UNNEST(JSON_QUERY_ARRAY(target_fingerprints)) b
      ON c.name=JSON_VALUE(b,'$.name') AND c.row_count=SAFE_CAST(JSON_VALUE(b,'$.row_count') AS INT64)
      AND c.fingerprint=JSON_VALUE(b,'$.fingerprint'))=6
      AS 'Retained rollback artifacts are incomplete or altered';

    DELETE `${project}.${curated}.cocoa` WHERE TRUE;
    INSERT `${project}.${curated}.cocoa` (Date,Euro_Price) SELECT * FROM restore_cocoa;
    DELETE `${project}.${curated}.brent_prices` WHERE TRUE;
    INSERT `${project}.${curated}.brent_prices` (date,brent_price_eu) SELECT * FROM restore_oil;
    DELETE `${project}.${curated}.precipitation` WHERE TRUE;
    INSERT `${project}.${curated}.precipitation` (date,precipitation,soil_moisture) SELECT * FROM restore_weather;
    DELETE `${project}.${curated}.joined_daily_cocoa_oil_weather` WHERE TRUE;
    INSERT `${project}.${curated}.joined_daily_cocoa_oil_weather` (date,cocoa_price,oil_price,precipitation,soil_moisture) SELECT * FROM restore_joined;
    DELETE `${project}.${curated}.forecast_40d` WHERE TRUE;
    INSERT `${project}.${curated}.forecast_40d` (forecast_date,forecast_cocoa_price) SELECT * FROM restore_forecast;
    DELETE `${project}.${curated}.pred_hist_40d` WHERE TRUE;
    INSERT `${project}.${curated}.pred_hist_40d` (prediction_date,forecast_date,yhat_40,actual) SELECT * FROM restore_predictions;
    UPDATE `${project}.${delivery}.active_release` SET release_id=p_target,business_date=target_date,published_at=CURRENT_TIMESTAMP() WHERE singleton=1;
    INSERT `${project}.${delivery}.release_events` VALUES(p_event_id,'rollback',p_expected_current,p_target,p_reason,CURRENT_TIMESTAMP());
    COMMIT TRANSACTION;
    SET in_transaction = FALSE;
  END IF;
  SELECT a.*,r.model_id,r.code_id FROM `${project}.${delivery}.active_release` a
    JOIN `${project}.${delivery}.release_runs` r USING(release_id);
  EXCEPTION WHEN ERROR THEN
    IF in_transaction THEN ROLLBACK TRANSACTION; END IF;
    RAISE;
  END;
END;
