-- Compatibility field names for the existing historical report charts.
-- All values come from the accepted release's bounded measurement policy.
CREATE OR REPLACE VIEW `${project}.${studio}.ls_market_history` AS
SELECT q.date,
  MAX(IF(q.measurement='cocoa_price',q.usable_value,NULL)) AS cocoa_price,
  MAX(IF(q.measurement='oil_price',q.usable_value,NULL)) AS oil_price,
  MAX(IF(q.measurement='precipitation',q.usable_value,NULL)) AS precipitation,
  MAX(IF(q.measurement='soil_moisture',q.usable_value,NULL)) AS soil_moisture
FROM `${project}.${studio}.ls_series_quality` q
JOIN `${project}.${delivery}.active_release` a USING(release_id,business_date)
WHERE a.singleton=1
GROUP BY q.date;

CREATE OR REPLACE VIEW `${project}.${studio}.ls_indexed_series_smooth` AS
WITH smoothed AS (
  SELECT date,cocoa_price,oil_price,
    IF(precipitation IS NULL,NULL,AVG(precipitation) OVER days) AS precip_ma7,
    IF(soil_moisture IS NULL,NULL,AVG(soil_moisture) OVER days) AS soil_ma7
  FROM `${project}.${studio}.ls_market_history`
  WINDOW days AS (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
), bounds AS (
  SELECT MIN(cocoa_price) AS min_cocoa,MAX(cocoa_price) AS max_cocoa,
    MIN(oil_price) AS min_oil,MAX(oil_price) AS max_oil,
    MIN(LN(1+GREATEST(precip_ma7,0))) AS min_p,
    MAX(LN(1+GREATEST(precip_ma7,0))) AS max_p
  FROM smoothed
)
SELECT s.date,
  100*(s.cocoa_price-b.min_cocoa)/NULLIF(b.max_cocoa-b.min_cocoa,0) AS cocoa_idx,
  100*(s.oil_price-b.min_oil)/NULLIF(b.max_oil-b.min_oil,0) AS oil_idx,
  100*(LN(1+GREATEST(s.precip_ma7,0))-b.min_p)/NULLIF(b.max_p-b.min_p,0) AS precip_idx,
  100*LEAST(GREATEST(s.soil_ma7,0),1) AS soil_pct
FROM smoothed s CROSS JOIN bounds b;
