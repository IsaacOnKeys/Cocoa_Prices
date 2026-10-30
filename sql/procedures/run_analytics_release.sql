-- Permanent model DDL and candidate computation precede atomic serving DML.
CREATE OR REPLACE PROCEDURE `${project}.${delivery}.run_daily_pipeline`(
  p_release_id STRING, p_business_date DATE, p_dag_run_id STRING,
  p_run_started_at TIMESTAMP, p_sources_json STRING, p_code_id STRING,
  p_failure_stage STRING
)
BEGIN
  DECLARE p_sources JSON DEFAULT PARSE_JSON(p_sources_json);
  DECLARE input_cutoff TIMESTAMP;
  DECLARE base_id STRING;
  DECLARE versioned_model STRING;
  DECLARE in_transaction BOOL DEFAULT FALSE;
  DECLARE is_published BOOL DEFAULT FALSE;
  DECLARE baseline JSON;
  DECLARE quality JSON;
  DECLARE published_time TIMESTAMP;
  DECLARE active_at_response BOOL DEFAULT FALSE;

  BEGIN
  ASSERT REGEXP_CONTAINS(p_release_id, r'^[a-f0-9]{64}$') AS 'Invalid release identity';
  ASSERT p_business_date IS NOT NULL AND p_run_started_at IS NOT NULL
    AND p_run_started_at <= CURRENT_TIMESTAMP()
    AND LENGTH(p_dag_run_id) BETWEEN 1 AND 1024
    AND REGEXP_CONTAINS(p_code_id, r'^[a-f0-9]{64}$') AS 'Invalid release scope/code';
  ASSERT p_failure_stage IS NULL OR (
    REGEXP_CONTAINS('${delivery}', r'^atomic_validation_[0-9]{8}_[a-f0-9]{8}$')
    AND p_failure_stage IN ('after_freeze', 'after_candidates', 'after_first_publish')
  ) AS 'Failure injection is restricted to isolated validation datasets';
  ASSERT (SELECT COUNT(*) FROM `${project}.${delivery}.release_runs`
    WHERE release_id=p_release_id) <= 1 AS 'Duplicate release registry identity';
  SET versioned_model = '${project}.${models}.cocoa_release_' || p_release_id;

  IF NOT EXISTS (SELECT 1 FROM `${project}.${delivery}.release_runs` WHERE release_id=p_release_id) THEN
    BEGIN TRANSACTION;
    SET in_transaction = TRUE;
    -- Establish a conflicting write for competing freeze/publication transactions.
    UPDATE `${project}.${delivery}.active_release` SET release_id=release_id WHERE singleton=1;
    ASSERT (SELECT COUNT(*) FROM `${project}.${delivery}.active_release`) = 1
      AND (SELECT COUNTIF(singleton=1) FROM `${project}.${delivery}.active_release`) = 1
      AS 'A verified baseline release must be installed first';
    SET base_id = (SELECT release_id FROM `${project}.${delivery}.active_release` WHERE singleton=1);
    SET input_cutoff = CURRENT_TIMESTAMP();

    CREATE TEMP TABLE freeze_fingerprints AS
    SELECT 'cocoa' AS name, COUNT(*) AS row_count,
      COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') AS fingerprint
      FROM `${project}.${curated}.cocoa` t
    UNION ALL SELECT 'brent_prices', COUNT(*), COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.brent_prices` t
    UNION ALL SELECT 'precipitation', COUNT(*), COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.precipitation` t
    UNION ALL SELECT 'joined_daily_cocoa_oil_weather', COUNT(*), COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.joined_daily_cocoa_oil_weather` t
    UNION ALL SELECT 'forecast_40d', COUNT(*), COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.forecast_40d` t
    UNION ALL SELECT 'pred_hist_40d', COUNT(*), COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.pred_hist_40d` t;
    SET baseline = (SELECT TO_JSON(ARRAY_AGG(STRUCT(name, row_count, fingerprint) ORDER BY name)) FROM freeze_fingerprints);

    INSERT `${project}.${delivery}.release_runs`
      (release_id,business_date,dag_run_id,run_started_at,input_cutoff,base_release_id,
       code_id,source_receipts,source_receipts_sha256,baseline_fingerprints,status,model_id,created_at)
    VALUES (p_release_id,p_business_date,p_dag_run_id,p_run_started_at,input_cutoff,base_id,
      p_code_id,p_sources,LOWER(TO_HEX(SHA256(p_sources_json))),baseline,'frozen',versioned_model,CURRENT_TIMESTAMP());

    INSERT `${project}.${delivery}.frozen_inputs`
      (release_id,business_date,kind,feed,date,price,precipitation,soil_moisture,
       acquisition_id,observation_id,producer_acquired_at,consumer_received_at,source_published_at,timestamp_status)
    SELECT p_release_id,p_business_date,'baseline','cocoa',Date,Euro_Price,
      CAST(NULL AS FLOAT64),CAST(NULL AS FLOAT64),CAST(NULL AS STRING),CAST(NULL AS STRING),
      CAST(NULL AS TIMESTAMP),CAST(NULL AS TIMESTAMP),CAST(NULL AS TIMESTAMP),CAST(NULL AS STRING)
      FROM `${project}.${curated}.cocoa` GROUP BY Date,Euro_Price
    UNION ALL SELECT p_release_id,p_business_date,'baseline','oil',date,brent_price_eu,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL FROM `${project}.${curated}.brent_prices` GROUP BY date,brent_price_eu
    UNION ALL SELECT p_release_id,p_business_date,'baseline','weather',date,NULL,precipitation,soil_moisture,NULL,NULL,NULL,NULL,NULL,NULL FROM `${project}.${curated}.precipitation` GROUP BY date,precipitation,soil_moisture
    UNION ALL
    SELECT p_release_id,p_business_date,'stage','cocoa',date,cocoa_price,NULL,NULL,
      acquisition_id,observation_id,producer_acquired_at,MIN(consumer_received_at),
      MIN(source_published_at),timestamp_status
      FROM `${project}.${staging}.cocoa_prices`
      WHERE acquisition_id=JSON_VALUE(p_sources,'$.cocoa.acquisition_id')
        AND producer_acquired_at=SAFE_CAST(JSON_VALUE(p_sources,'$.cocoa.acquired_at') AS TIMESTAMP)
        AND consumer_received_at<=input_cutoff
      GROUP BY date,observation_id,cocoa_price,acquisition_id,producer_acquired_at,timestamp_status
    UNION ALL
    SELECT p_release_id,p_business_date,'stage','oil',date,oil_price,NULL,NULL,
      acquisition_id,observation_id,producer_acquired_at,MIN(consumer_received_at),
      MIN(source_published_at),timestamp_status
      FROM `${project}.${staging}.oil_prices`
      WHERE acquisition_id=JSON_VALUE(p_sources,'$.oil.acquisition_id')
        AND producer_acquired_at=SAFE_CAST(JSON_VALUE(p_sources,'$.oil.acquired_at') AS TIMESTAMP)
        AND consumer_received_at<=input_cutoff
      GROUP BY date,observation_id,oil_price,acquisition_id,producer_acquired_at,timestamp_status
    UNION ALL
    SELECT p_release_id,p_business_date,'stage','weather',date,NULL,precipitation,soil_moisture,
      acquisition_id,observation_id,producer_acquired_at,MIN(consumer_received_at),
      MIN(source_published_at),timestamp_status
      FROM `${project}.${staging}.precipitation_moisture`
      WHERE acquisition_id=JSON_VALUE(p_sources,'$.weather.acquisition_id')
        AND producer_acquired_at=SAFE_CAST(JSON_VALUE(p_sources,'$.weather.acquired_at') AS TIMESTAMP)
        AND consumer_received_at<=input_cutoff
      GROUP BY date,observation_id,precipitation,soil_moisture,acquisition_id,producer_acquired_at,timestamp_status;
    COMMIT TRANSACTION;
    SET in_transaction = FALSE;
  ELSE
    ASSERT (SELECT COUNT(*) FROM `${project}.${delivery}.release_runs`
      WHERE release_id=p_release_id AND business_date=p_business_date AND dag_run_id=p_dag_run_id
        AND run_started_at=p_run_started_at AND code_id=p_code_id
        AND source_receipts_sha256=LOWER(TO_HEX(SHA256(p_sources_json)))) = 1
      AS 'Retry scope/code/source receipts differ from the frozen release';
    SET input_cutoff = (SELECT r.input_cutoff FROM `${project}.${delivery}.release_runs` r WHERE release_id=p_release_id);
    SET base_id = (SELECT base_release_id FROM `${project}.${delivery}.release_runs` WHERE release_id=p_release_id);
    SET baseline = (SELECT baseline_fingerprints FROM `${project}.${delivery}.release_runs` WHERE release_id=p_release_id);
    SET is_published = (SELECT status='published' FROM `${project}.${delivery}.release_runs` WHERE release_id=p_release_id);
    IF is_published THEN
      SET published_time = (SELECT published_at FROM `${project}.${delivery}.release_runs` WHERE release_id=p_release_id);
      SET active_at_response = (SELECT release_id=p_release_id FROM `${project}.${delivery}.active_release` WHERE singleton=1);
    END IF;
  END IF;

  IF NOT is_published THEN
    ASSERT p_failure_stage IS DISTINCT FROM 'after_freeze' AS 'Injected failure after frozen inputs';
    CREATE TEMP TABLE source_input AS
      SELECT * EXCEPT(release_id,business_date) FROM `${project}.${delivery}.frozen_inputs`
      WHERE release_id=p_release_id AND business_date=p_business_date;
    ${input_quality_checks}

    ${revision_selection}

    ${imputation_join}

    CREATE TEMP TABLE candidate_features AS
    SELECT date,cocoa_price AS y,
      LAG(cocoa_price,1) OVER (ORDER BY date) AS y_lag1,
      LAG(cocoa_price,7) OVER (ORDER BY date) AS y_lag7,
      LAG(cocoa_price,28) OVER (ORDER BY date) AS y_lag28,
      AVG(oil_price) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS oil_ma7,
      AVG(precipitation) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS precip_ma7,
      AVG(soil_moisture) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS soil_ma7,
      CAST(EXTRACT(DAYOFWEEK FROM date) AS STRING) AS dow,
      CAST(EXTRACT(MONTH FROM date) AS STRING) AS mon,
      LEAD(cocoa_price,40) OVER (ORDER BY date) AS target_40
    FROM candidate_joined;
    ASSERT (SELECT COUNTIF(target_40 IS NOT NULL) FROM candidate_features)>0
      AS 'No eligible training labels; withhold the entire release';

    EXECUTE IMMEDIATE FORMAT('''CREATE MODEL IF NOT EXISTS `%s`
      OPTIONS(model_type='linear_reg',input_label_cols=['target_40'],l2_reg=1.0,data_split_method='AUTO_SPLIT')
      AS SELECT * EXCEPT(date) FROM _SESSION.candidate_features WHERE target_40 IS NOT NULL''',versioned_model);
    EXECUTE IMMEDIATE FORMAT('''CREATE TEMP TABLE candidate_forecast AS
      SELECT DATE_ADD(date,INTERVAL 40 DAY) AS forecast_date,predicted_target_40 AS forecast_cocoa_price
      FROM ML.PREDICT(MODEL `%s`,(SELECT * EXCEPT(target_40) FROM _SESSION.candidate_features
        WHERE date>=DATE_SUB(@business_date,INTERVAL 39 DAY)))''',versioned_model)
      USING p_business_date AS business_date;
    EXECUTE IMMEDIATE FORMAT('''CREATE TEMP TABLE candidate_predictions AS
      SELECT p.date AS prediction_date,DATE_ADD(p.date,INTERVAL 40 DAY) AS forecast_date,
        p.predicted_target_40 AS yhat_40,f.y AS actual
      FROM ML.PREDICT(MODEL `%s`,(SELECT * EXCEPT(target_40) FROM _SESSION.candidate_features)) p
      LEFT JOIN _SESSION.candidate_features f ON f.date=DATE_ADD(p.date,INTERVAL 40 DAY)''',versioned_model);

    ${output_quality_checks}
    SET quality = TO_JSON(STRUCT(
      (SELECT COUNT(*) FROM candidate_cocoa) AS cocoa_rows,
      (SELECT COUNT(*) FROM candidate_oil) AS oil_rows,
      (SELECT COUNT(*) FROM candidate_weather) AS weather_rows,
      (SELECT COUNT(*) FROM candidate_joined) AS calendar_rows,
      (SELECT COUNT(*) FROM candidate_forecast) AS forecast_rows,
      TRUE AS current_acquisitions_reconciled,TRUE AS forecast_contract_passed,
      STRUCT('accepted_non_null_v1' AS version,'preserve_known_value' AS null_policy,
        'reject_conflicting_ties' AS conflict_policy) AS revision_policy,
      (SELECT COUNTIF(s.price IS NULL AND b.price IS NOT NULL)
        +COUNTIF(s.precipitation IS NULL AND b.precipitation IS NOT NULL AND b.precipitation!=-999.0)
        +COUNTIF(s.soil_moisture IS NULL AND b.soil_moisture IS NOT NULL AND b.soil_moisture!=-999.0)
        FROM source_input s JOIN source_input b USING(feed,date)
        WHERE s.kind='stage' AND b.kind='baseline') AS preserved_missing_measurements,
      STRUCT('bounded_calendar_v1' AS version,7 AS cocoa_days,14 AS oil_days,
        7 AS precipitation_days,7 AS soil_moisture_days) AS imputation_policy,
      (SELECT ARRAY_AGG(STRUCT(stage,check_name,severity,violations,detail) ORDER BY stage,check_name) FROM quality_checks) AS checks,
      (SELECT COUNTIF(severity='warning' AND violations>0) FROM quality_checks) AS warning_checks,
      (SELECT ARRAY_AGG(STRUCT(name,row_count,fingerprint) ORDER BY name) FROM (
        SELECT 'cocoa' AS name,COUNT(*) AS row_count,COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') AS fingerprint FROM candidate_cocoa t
        UNION ALL SELECT 'brent_prices',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM candidate_oil t
        UNION ALL SELECT 'precipitation',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM candidate_weather t
        UNION ALL SELECT 'joined_daily_cocoa_oil_weather',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM candidate_joined t
        UNION ALL SELECT 'forecast_40d',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM candidate_forecast t
        UNION ALL SELECT 'pred_hist_40d',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM candidate_predictions t
      )) AS artifacts));

    BEGIN TRANSACTION;
    SET in_transaction = TRUE;
    -- A retry replaces only this release's candidate partitions, never live facts.
    DELETE `${project}.${delivery}.cocoa` WHERE release_id=p_release_id AND business_date=p_business_date;
    INSERT `${project}.${delivery}.cocoa` SELECT p_release_id,p_business_date,* FROM candidate_cocoa;
    DELETE `${project}.${delivery}.brent_prices` WHERE release_id=p_release_id AND business_date=p_business_date;
    INSERT `${project}.${delivery}.brent_prices` SELECT p_release_id,p_business_date,* FROM candidate_oil;
    DELETE `${project}.${delivery}.precipitation` WHERE release_id=p_release_id AND business_date=p_business_date;
    INSERT `${project}.${delivery}.precipitation` SELECT p_release_id,p_business_date,* FROM candidate_weather;
    DELETE `${project}.${delivery}.joined_daily_cocoa_oil_weather` WHERE release_id=p_release_id AND business_date=p_business_date;
    INSERT `${project}.${delivery}.joined_daily_cocoa_oil_weather` SELECT p_release_id,p_business_date,* FROM candidate_joined;
    DELETE `${project}.${delivery}.features` WHERE release_id=p_release_id AND business_date=p_business_date;
    INSERT `${project}.${delivery}.features` SELECT p_release_id,p_business_date,* FROM candidate_features;
    DELETE `${project}.${delivery}.forecast_40d` WHERE release_id=p_release_id AND business_date=p_business_date;
    INSERT `${project}.${delivery}.forecast_40d` SELECT p_release_id,p_business_date,* FROM candidate_forecast;
    DELETE `${project}.${delivery}.pred_hist_40d` WHERE release_id=p_release_id AND business_date=p_business_date;
    INSERT `${project}.${delivery}.pred_hist_40d` SELECT p_release_id,p_business_date,* FROM candidate_predictions;
    UPDATE `${project}.${delivery}.release_runs` SET status='validated',quality_results=quality,
      validated_at=CURRENT_TIMESTAMP(),last_error=NULL WHERE release_id=p_release_id AND business_date=p_business_date;
    COMMIT TRANSACTION;
    SET in_transaction = FALSE;
    ASSERT p_failure_stage IS DISTINCT FROM 'after_candidates' AS 'Injected failure after isolated candidates';

    BEGIN TRANSACTION;
    SET in_transaction = TRUE;
    ASSERT (SELECT release_id FROM `${project}.${delivery}.active_release` WHERE singleton=1)=base_id
      AS 'Another release was published; start a new logical run';
    CREATE TEMP TABLE current_fingerprints AS
    SELECT 'cocoa' AS name,COUNT(*) AS row_count,COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') AS fingerprint FROM `${project}.${curated}.cocoa` t
    UNION ALL SELECT 'brent_prices',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.brent_prices` t
    UNION ALL SELECT 'precipitation',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.precipitation` t
    UNION ALL SELECT 'joined_daily_cocoa_oil_weather',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.joined_daily_cocoa_oil_weather` t
    UNION ALL SELECT 'forecast_40d',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.forecast_40d` t
    UNION ALL SELECT 'pred_hist_40d',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.pred_hist_40d` t;
    ASSERT (SELECT COUNT(*) FROM current_fingerprints c JOIN UNNEST(JSON_QUERY_ARRAY(baseline)) b
      ON c.name=JSON_VALUE(b,'$.name') AND c.row_count=SAFE_CAST(JSON_VALUE(b,'$.row_count') AS INT64)
      AND c.fingerprint=JSON_VALUE(b,'$.fingerprint'))=6
      AS 'Serving data changed outside the release workflow; reconcile before publication';

    DELETE `${project}.${curated}.cocoa` WHERE TRUE;
    INSERT `${project}.${curated}.cocoa` (Date,Euro_Price) SELECT Date,Euro_Price FROM candidate_cocoa;
    ASSERT p_failure_stage IS DISTINCT FROM 'after_first_publish' AS 'Injected failure inside serving transaction';
    DELETE `${project}.${curated}.brent_prices` WHERE TRUE;
    INSERT `${project}.${curated}.brent_prices` (date,brent_price_eu) SELECT date,brent_price_eu FROM candidate_oil;
    DELETE `${project}.${curated}.precipitation` WHERE TRUE;
    INSERT `${project}.${curated}.precipitation` (date,precipitation,soil_moisture) SELECT date,precipitation,soil_moisture FROM candidate_weather;
    DELETE `${project}.${curated}.joined_daily_cocoa_oil_weather` WHERE TRUE;
    INSERT `${project}.${curated}.joined_daily_cocoa_oil_weather` (date,cocoa_price,oil_price,precipitation,soil_moisture)
      SELECT date,cocoa_price,oil_price,precipitation,soil_moisture FROM candidate_joined;
    DELETE `${project}.${curated}.forecast_40d` WHERE TRUE;
    INSERT `${project}.${curated}.forecast_40d` (forecast_date,forecast_cocoa_price) SELECT * FROM candidate_forecast;
    DELETE `${project}.${curated}.pred_hist_40d` WHERE TRUE;
    INSERT `${project}.${curated}.pred_hist_40d` (prediction_date,forecast_date,yhat_40,actual) SELECT * FROM candidate_predictions;
    SET published_time = CURRENT_TIMESTAMP();
    UPDATE `${project}.${delivery}.active_release` SET release_id=p_release_id,business_date=p_business_date,
      published_at=published_time WHERE singleton=1;
    UPDATE `${project}.${delivery}.release_runs` SET status='published',published_at=published_time,last_error=NULL
      WHERE release_id=p_release_id AND business_date=p_business_date;
    COMMIT TRANSACTION;
    SET in_transaction = FALSE;
    SET active_at_response = TRUE;
  END IF;
  -- Returning a receipt must not add a billed permanent-table read after commit.
  SELECT p_release_id AS release_id,p_business_date AS business_date,'published' AS status,
    versioned_model AS model_id,input_cutoff AS input_cutoff,published_time AS published_at,
    active_at_response AS is_active;
EXCEPTION WHEN ERROR THEN
  IF in_transaction THEN
    ROLLBACK TRANSACTION;
  END IF;
  UPDATE `${project}.${delivery}.release_runs` SET status='failed',last_error=LEFT(@@error.message,1024)
    WHERE release_id=p_release_id AND business_date=p_business_date AND status!='published';
  RAISE;
  END;
END;
