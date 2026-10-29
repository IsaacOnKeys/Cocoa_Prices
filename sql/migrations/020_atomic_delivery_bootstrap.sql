-- Explicit one-time seed from an independently verified, already served release.
-- Never infer a successful business date from CURRENT_DATE or staging ingestion.
ASSERT REGEXP_CONTAINS(@release_id,r'^[a-f0-9]{64}$')
  AND @business_date IS NOT NULL AND REGEXP_CONTAINS(@code_id,r'^[a-f0-9]{64}$')
  AS 'Invalid verified baseline identity';
BEGIN TRANSACTION;
ASSERT (SELECT COUNT(*) FROM `${project}.${delivery}.active_release`)=0
  AND (SELECT COUNT(*) FROM `${project}.${delivery}.release_runs`)=0
  AS 'Bootstrap requires an empty release registry';
CREATE TEMP TABLE bootstrap_fingerprints AS
SELECT 'cocoa' AS name,COUNT(*) AS row_count,COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') AS fingerprint FROM `${project}.${curated}.cocoa` t
UNION ALL SELECT 'brent_prices',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.brent_prices` t
UNION ALL SELECT 'precipitation',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.precipitation` t
UNION ALL SELECT 'joined_daily_cocoa_oil_weather',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.joined_daily_cocoa_oil_weather` t
UNION ALL SELECT 'forecast_40d',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.forecast_40d` t
UNION ALL SELECT 'pred_hist_40d',COUNT(*),COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') FROM `${project}.${curated}.pred_hist_40d` t;
ASSERT (SELECT COUNT(*) FROM bootstrap_fingerprints c JOIN UNNEST(JSON_QUERY_ARRAY(@fingerprints)) b
  ON c.name=JSON_VALUE(b,'$.name') AND c.row_count=SAFE_CAST(JSON_VALUE(b,'$.row_count') AS INT64)
  AND c.fingerprint=JSON_VALUE(b,'$.fingerprint'))=6
  AS 'Verified baseline changed before bootstrap';

INSERT `${project}.${delivery}.cocoa` SELECT @release_id,@business_date,* FROM `${project}.${curated}.cocoa`;
INSERT `${project}.${delivery}.brent_prices` SELECT @release_id,@business_date,* FROM `${project}.${curated}.brent_prices`;
INSERT `${project}.${delivery}.precipitation` SELECT @release_id,@business_date,* FROM `${project}.${curated}.precipitation`;
INSERT `${project}.${delivery}.joined_daily_cocoa_oil_weather` SELECT @release_id,@business_date,* FROM `${project}.${curated}.joined_daily_cocoa_oil_weather`;
INSERT `${project}.${delivery}.forecast_40d` SELECT @release_id,@business_date,* FROM `${project}.${curated}.forecast_40d`;
INSERT `${project}.${delivery}.pred_hist_40d` SELECT @release_id,@business_date,* FROM `${project}.${curated}.pred_hist_40d`;
INSERT `${project}.${delivery}.features` SELECT @release_id,@business_date,* FROM `${project}.${curated}.features_live`;
INSERT `${project}.${delivery}.release_runs`
  (release_id,business_date,dag_run_id,code_id,source_receipts,baseline_fingerprints,status,
   model_id,quality_results,created_at,validated_at)
VALUES (@release_id,@business_date,'verified_legacy_baseline',@code_id,@evidence,@fingerprints,
  'legacy_verified',@model_id,TO_JSON(STRUCT('verified_legacy_baseline' AS classification,
    @fingerprints AS artifacts)),CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP());
-- The original publication timestamp is unknown: do not invent a new delivery.
INSERT `${project}.${delivery}.active_release` VALUES (1,@release_id,@business_date,NULL);
COMMIT TRANSACTION;
