-- Calendar-day carry limits match the contracted source-age limits, inclusively.
-- A missing row/null/sentinel never resets the last valid measurement's date.
CREATE TEMP TABLE candidate_joined AS
WITH raw AS (
  SELECT d AS date,c.Euro_Price AS cocoa_raw,o.brent_price_eu AS oil_raw,
    NULLIF(w.precipitation,-999.0) AS precipitation_raw,
    NULLIF(w.soil_moisture,-999.0) AS soil_raw
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2014-01-02',p_business_date)) d
  LEFT JOIN candidate_cocoa c ON d=c.Date
  LEFT JOIN candidate_oil o ON d=o.date
  LEFT JOIN candidate_weather w ON d=w.date
), carried AS (
  SELECT date,
    LAST_VALUE(IF(cocoa_raw IS NULL,NULL,STRUCT(date AS observed_on,cocoa_raw AS value)) IGNORE NULLS) OVER dates AS cocoa,
    LAST_VALUE(IF(oil_raw IS NULL,NULL,STRUCT(date AS observed_on,oil_raw AS value)) IGNORE NULLS) OVER dates AS oil,
    LAST_VALUE(IF(precipitation_raw IS NULL,NULL,STRUCT(date AS observed_on,precipitation_raw AS value)) IGNORE NULLS) OVER dates AS rain,
    LAST_VALUE(IF(soil_raw IS NULL,NULL,STRUCT(date AS observed_on,soil_raw AS value)) IGNORE NULLS) OVER dates AS soil
  FROM raw WINDOW dates AS (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT date,
  IF(DATE_DIFF(date,cocoa.observed_on,DAY)<=7,cocoa.value,NULL) AS cocoa_price,
  IF(DATE_DIFF(date,oil.observed_on,DAY)<=14,oil.value,NULL) AS oil_price,
  IF(DATE_DIFF(date,rain.observed_on,DAY)<=7,rain.value,NULL) AS precipitation,
  IF(DATE_DIFF(date,soil.observed_on,DAY)<=7,soil.value,NULL) AS soil_moisture
FROM carried;
