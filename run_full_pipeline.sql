CREATE OR REPLACE PROCEDURE `cocoa-prices-430315.cocoa_related.run_daily_pipeline`()
BEGIN

-- 1) Update the 3 tables individually (dedupe by latest ingestion_time per date)

MERGE `cocoa-prices-430315.cocoa_related.cocoa` T
USING (
  SELECT date, cocoa_price
  FROM `cocoa-prices-430315.stream_staging.cocoa_prices`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY ingestion_time DESC) = 1
) S
ON T.Date = S.date
WHEN MATCHED THEN
  UPDATE SET Euro_Price = S.cocoa_price
WHEN NOT MATCHED THEN
  INSERT (Date, Euro_Price) VALUES (S.date, S.cocoa_price);

MERGE `cocoa-prices-430315.cocoa_related.brent_prices` T
USING (
  SELECT date, oil_price
  FROM `cocoa-prices-430315.stream_staging.oil_prices`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY ingestion_time DESC) = 1
) S
ON T.date = S.date
WHEN MATCHED THEN
  UPDATE SET brent_price_eu = S.oil_price
WHEN NOT MATCHED THEN
  INSERT (date, brent_price_eu) VALUES (S.date, S.oil_price);

MERGE `cocoa-prices-430315.cocoa_related.precipitation` T
USING (
  SELECT date, precipitation, soil_moisture
  FROM `cocoa-prices-430315.stream_staging.precipitation_moisture`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY date ORDER BY ingestion_time DESC) = 1
) S
ON T.date = S.date
WHEN MATCHED THEN
  UPDATE SET
    precipitation = S.precipitation,
    soil_moisture = S.soil_moisture
WHEN NOT MATCHED THEN
  INSERT (date, precipitation, soil_moisture)
  VALUES (S.date, S.precipitation, S.soil_moisture);

-- 2) Join + forward-fill
CREATE OR REPLACE TABLE `cocoa-prices-430315.cocoa_related.joined_daily_cocoa_oil_weather` AS
WITH cal AS (
  SELECT d
  FROM UNNEST(GENERATE_DATE_ARRAY(
    DATE '2014-01-01',
    COALESCE((SELECT MAX(mx) FROM UNNEST([
      (SELECT MAX(Date) FROM `cocoa-prices-430315.cocoa_related.cocoa`),
      (SELECT MAX(date) FROM `cocoa-prices-430315.cocoa_related.brent_prices`),
      (SELECT MAX(date) FROM `cocoa-prices-430315.cocoa_related.precipitation`)
    ]) AS mx), CURRENT_DATE())
  )) AS d
),
j AS (
  SELECT
    cal.d AS date,
    c.Euro_Price     AS cocoa_price_raw,
    o.brent_price_eu AS oil_price_raw,
    p.precipitation,
    p.soil_moisture
  FROM cal
  LEFT JOIN (
    SELECT Date, ANY_VALUE(Euro_Price) AS Euro_Price
    FROM `cocoa-prices-430315.cocoa_related.cocoa`
    GROUP BY Date
  ) c ON cal.d = c.Date
  LEFT JOIN (
    SELECT date, ANY_VALUE(brent_price_eu) AS brent_price_eu
    FROM `cocoa-prices-430315.cocoa_related.brent_prices`
    GROUP BY date
  ) o ON cal.d = o.date
  LEFT JOIN (
    SELECT date,
           ANY_VALUE(precipitation) AS precipitation,
           ANY_VALUE(soil_moisture) AS soil_moisture
    FROM `cocoa-prices-430315.cocoa_related.precipitation`
    GROUP BY date
  ) p ON cal.d = p.date
)
SELECT
  date,
  LAST_VALUE(cocoa_price_raw IGNORE NULLS) OVER (ORDER BY date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cocoa_price,
  LAST_VALUE(oil_price_raw   IGNORE NULLS) OVER (ORDER BY date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS oil_price,
  LAST_VALUE(precipitation   IGNORE NULLS) OVER (ORDER BY date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS precipitation,
  LAST_VALUE(soil_moisture   IGNORE NULLS) OVER (ORDER BY date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS soil_moisture
FROM j;


-- 3) Features (lags + 7-day MAs) + target_40
CREATE OR REPLACE VIEW `cocoa-prices-430315.cocoa_related.features_live` AS
WITH s AS (
  SELECT
    date,
    cocoa_price AS y,
    oil_price,
    precipitation,
    soil_moisture
  FROM `cocoa-prices-430315.cocoa_related.joined_daily_cocoa_oil_weather`
)
SELECT
  date,
  y,
  LAG(y, 1)  OVER (ORDER BY date) AS y_lag1,
  LAG(y, 7)  OVER (ORDER BY date) AS y_lag7,
  LAG(y, 28) OVER (ORDER BY date) AS y_lag28,
  AVG(oil_price)     OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS oil_ma7,
  AVG(precipitation) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS precip_ma7,
  AVG(soil_moisture) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS soil_ma7,
  CAST(EXTRACT(DAYOFWEEK FROM date) AS STRING) AS dow,
  CAST(EXTRACT(MONTH     FROM date) AS STRING) AS mon,
  LEAD(y, 40) OVER (ORDER BY date) AS target_40
FROM s;

-- 4–7) Model + forecasts + backtest + metric (guarded)
IF (SELECT COUNT(*) FROM `cocoa-prices-430315.cocoa_related.features_live` WHERE target_40 IS NOT NULL) > 0 THEN

  -- 4) Model (ridge; no tuning)
  CREATE OR REPLACE MODEL `cocoa-prices-430315.models.cocoa_linreg_40`
  OPTIONS(
    model_type = 'linear_reg',
    input_label_cols = ['target_40'],
    l2_reg = 1.0,
    data_split_method = 'AUTO_SPLIT'
  ) AS
  SELECT * EXCEPT(date)
  FROM `cocoa-prices-430315.cocoa_related.features_live`
  WHERE target_40 IS NOT NULL;

  -- 5) Forecasts (40-day ahead) for Looker
  CREATE OR REPLACE TABLE `cocoa-prices-430315.cocoa_related.forecast_40d` AS
  SELECT
    DATE_ADD(p.date, INTERVAL 40 DAY) AS forecast_date,
    p.predicted_target_40             AS forecast_cocoa_price
  FROM ML.PREDICT(
         MODEL `cocoa-prices-430315.models.cocoa_linreg_40`,
         (SELECT * EXCEPT(target_40)
          FROM `cocoa-prices-430315.cocoa_related.features_live`
          WHERE date >= DATE_SUB(
            (SELECT MAX(date)
             FROM `cocoa-prices-430315.cocoa_related.features_live`),
            INTERVAL 39 DAY))
  ) AS p;

  -- 6) Backtest history + MAPE
  CREATE OR REPLACE TABLE `cocoa-prices-430315.cocoa_related.pred_hist_40d` AS
  WITH raw_preds AS (
    SELECT
      date AS prediction_date,
      DATE_ADD(date, INTERVAL 40 DAY) AS forecast_date,
      predicted_target_40 AS yhat_40
    FROM ML.PREDICT(
            MODEL `cocoa-prices-430315.models.cocoa_linreg_40`,
            (SELECT * EXCEPT(target_40)
             FROM `cocoa-prices-430315.cocoa_related.features_live`))
  )
  SELECT
    r.prediction_date,
    r.forecast_date,
    r.yhat_40,
    f.y AS actual
  FROM raw_preds r
  LEFT JOIN `cocoa-prices-430315.cocoa_related.features_live` f
    ON r.forecast_date = f.date
  ORDER BY r.forecast_date;

  -- 7) Single-value metric: MAPE over 40-day backtest
  CREATE OR REPLACE VIEW `cocoa-prices-430315.cocoa_related.mape_40d` AS
  SELECT
    AVG(SAFE_DIVIDE(ABS(actual - yhat_40), actual))*100 AS mape_40d
  FROM `cocoa-prices-430315.cocoa_related.pred_hist_40d`
  WHERE actual IS NOT NULL;

END IF;
END;