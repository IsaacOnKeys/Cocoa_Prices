-- Called inside the existing freeze transaction. Stage/current-acquisition
-- precedence is unchanged; this path only admits closed history (>90 days).
CREATE TEMP TABLE approved_history AS
SELECT repair_id,PARSE_JSON(plan_json) AS plan FROM `${project}.${delivery}.historical_repairs`
WHERE status='approved' AND approved_at<=input_cutoff;

ASSERT (SELECT COUNT(*) FROM approved_history
  WHERE JSON_VALUE(plan,'$.target.delivery') IS DISTINCT FROM '${delivery}'
    OR JSON_VALUE(plan,'$.target.curated') IS DISTINCT FROM '${curated}'
    OR SAFE_CAST(JSON_VALUE(plan,'$.version') AS INT64) IS DISTINCT FROM 1
    OR IFNULL(JSON_VALUE(plan,'$.feed'),'') NOT IN ('cocoa','oil','weather')
    OR IFNULL(JSON_TYPE(JSON_QUERY(plan,'$.rows')),'missing')!='array'
    OR ARRAY_LENGTH(JSON_QUERY_ARRAY(plan,'$.rows')) NOT BETWEEN 1 AND 366)=0
  AS 'Invalid historical repair scope/shape';
ASSERT (SELECT COUNT(*) FROM `${project}.${delivery}.historical_repairs`
  WHERE status='approved' AND approved_at<=input_cutoff
    AND repair_id IS DISTINCT FROM LOWER(TO_HEX(SHA256(plan_json))))=0
  AS 'Historical repair content differs from its reviewed identity';

CREATE TEMP TABLE historical_changes AS
SELECT repair_id,JSON_VALUE(plan,'$.feed') AS feed,
  SAFE_CAST(JSON_VALUE(r,'$.date') AS DATE) AS date,
  SAFE_CAST(JSON_VALUE(plan,'$.start') AS DATE) AS interval_start,
  SAFE_CAST(JSON_VALUE(plan,'$.end') AS DATE) AS interval_end,
  SAFE_CAST(JSON_VALUE(r,'$.before_exists') AS BOOL) AS before_exists,
  SAFE_CAST(JSON_VALUE(r,'$.before.price') AS FLOAT64) AS before_price,
  SAFE_CAST(JSON_VALUE(r,'$.before.precipitation') AS FLOAT64) AS before_precipitation,
  SAFE_CAST(JSON_VALUE(r,'$.before.soil_moisture') AS FLOAT64) AS before_soil_moisture,
  SAFE_CAST(JSON_VALUE(r,'$.proposed.price') AS FLOAT64) AS price,
  SAFE_CAST(JSON_VALUE(r,'$.proposed.precipitation') AS FLOAT64) AS precipitation,
  SAFE_CAST(JSON_VALUE(r,'$.proposed.soil_moisture') AS FLOAT64) AS soil_moisture,
  (IFNULL(JSON_TYPE(JSON_QUERY(r,'$.before.price')),'missing') IN ('number','null')
   AND IFNULL(JSON_TYPE(JSON_QUERY(r,'$.before.precipitation')),'missing') IN ('number','null')
   AND IFNULL(JSON_TYPE(JSON_QUERY(r,'$.before.soil_moisture')),'missing') IN ('number','null')
   AND IFNULL(JSON_TYPE(JSON_QUERY(r,'$.proposed.price')),'missing') IN ('number','null')
   AND IFNULL(JSON_TYPE(JSON_QUERY(r,'$.proposed.precipitation')),'missing') IN ('number','null')
   AND IFNULL(JSON_TYPE(JSON_QUERY(r,'$.proposed.soil_moisture')),'missing') IN ('number','null')) AS valid_numbers
FROM approved_history,UNNEST(JSON_QUERY_ARRAY(plan,'$.rows')) r;

ASSERT (SELECT COUNT(*) FROM historical_changes WHERE date IS NULL OR interval_start IS NULL
  OR interval_end IS NULL OR before_exists IS NULL OR valid_numbers IS DISTINCT FROM TRUE
  OR date<DATE '2014-01-01' OR date NOT BETWEEN interval_start AND interval_end
  OR DATE_DIFF(interval_end,interval_start,DAY) NOT BETWEEN 0 AND 365
  OR interval_end>=DATE_SUB(p_business_date,INTERVAL 90 DAY))=0
  AS 'Historical repairs require a bounded closed interval older than 90 days';
ASSERT (SELECT COUNT(*) FROM (SELECT feed,date FROM historical_changes GROUP BY feed,date HAVING COUNT(*)!=1))=0
  AS 'Historical repairs overlap; cancel/replan before publication';
ASSERT (SELECT COUNT(*) FROM historical_changes c LEFT JOIN `${project}.${delivery}.frozen_inputs` b
  ON b.release_id=p_release_id AND b.business_date=p_business_date AND b.kind='baseline' AND b.feed=c.feed AND b.date=c.date
  WHERE (b.date IS NOT NULL) IS DISTINCT FROM c.before_exists
    OR b.price IS DISTINCT FROM c.before_price OR b.precipitation IS DISTINCT FROM c.before_precipitation
    OR b.soil_moisture IS DISTINCT FROM c.before_soil_moisture)=0
  AS 'Historical baseline changed; cancel or replan the reviewed repair';

MERGE `${project}.${delivery}.frozen_inputs` b USING historical_changes c
ON b.release_id=p_release_id AND b.business_date=p_business_date AND b.kind='baseline' AND b.feed=c.feed AND b.date=c.date
WHEN MATCHED THEN UPDATE SET price=COALESCE(c.price,b.price),
  precipitation=COALESCE(c.precipitation,b.precipitation),soil_moisture=COALESCE(c.soil_moisture,b.soil_moisture),
  historical_repair_id=c.repair_id
WHEN NOT MATCHED THEN INSERT
  (release_id,business_date,kind,feed,date,price,precipitation,soil_moisture,historical_repair_id)
  VALUES (p_release_id,p_business_date,'baseline',c.feed,c.date,c.price,c.precipitation,c.soil_moisture,c.repair_id);
