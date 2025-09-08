CREATE OR REPLACE PROCEDURE `cocoa-prices-430315.cocoa_related.run_feature_build`()
BEGIN
  -- 1) Update the 3 tables individually
  MERGE `cocoa-prices-430315.cocoa_related.cocoa` T
  USING `cocoa-prices-430315.cocoa_related.cocoa_temp` S
  ON T.Date = S.Date
  WHEN MATCHED THEN
    UPDATE SET Euro_Price = S.Euro_Price
  WHEN NOT MATCHED THEN
    INSERT (Date, Euro_Price)
    VALUES (S.Date, S.Euro_Price);

  MERGE `cocoa-prices-430315.cocoa_related.brent_prices` T
  USING `cocoa-prices-430315.cocoa_related.brent_prices_temp` S
  ON T.date = S.date
  WHEN MATCHED THEN
    UPDATE SET brent_price_eu = S.brent_price_eu
  WHEN NOT MATCHED THEN
    INSERT (date, brent_price_eu)
    VALUES (S.date, S.brent_price_eu);

  MERGE `cocoa-prices-430315.cocoa_related.precipitation` T
  USING `cocoa-prices-430315.cocoa_related.precipitation_temp` S
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
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2014-01-01', CURRENT_DATE())) AS d
  ),
  j AS (
    SELECT
      cal.d AS date,
      c.Euro_Price     AS cocoa_price_raw,
      o.brent_price_eu AS oil_price_raw,
      p.precipitation,
      p.soil_moisture
    FROM cal
    LEFT JOIN `cocoa-prices-430315.cocoa_related.cocoa`         c ON cal.d = c.date
    LEFT JOIN `cocoa-prices-430315.cocoa_related.brent_prices`  o ON cal.d = o.date
    LEFT JOIN `cocoa-prices-430315.cocoa_related.precipitation` p ON cal.d = p.date
  )
  SELECT
    date,
    LAST_VALUE(cocoa_price_raw IGNORE NULLS) OVER (ORDER BY date) AS cocoa_price,
    LAST_VALUE(oil_price_raw   IGNORE NULLS) OVER (ORDER BY date) AS oil_price,
    LAST_VALUE(precipitation   IGNORE NULLS) OVER (ORDER BY date) AS precipitation,
    LAST_VALUE(soil_moisture   IGNORE NULLS) OVER (ORDER BY date) AS soil_moisture
  FROM j
  ORDER BY date;

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
    AVG(oil_price)         OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS oil_ma7,
    AVG(precipitation)     OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS precip_ma7,
    AVG(soil_moisture)     OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS soil_ma7,
    CAST(EXTRACT(DAYOFWEEK FROM date) AS STRING) AS dow,
    CAST(EXTRACT(MONTH     FROM date) AS STRING) AS mon,
    LEAD(y, 40) OVER (ORDER BY date) AS target_40
  FROM s;

  -- 4) Model (ridge; no tuning)
  CREATE OR REPLACE MODEL `cocoa-prices-430315.models.cocoa_linreg_40`
  OPTIONS(
    model_type = 'linear_reg',
    input_label_cols = ['target_40'],
    l2_reg = 1.0,
    data_split_method = 'AUTO_SPLIT'
  ) AS
  SELECT *
  EXCEPT(date)
  FROM `cocoa-prices-430315.cocoa_related.features_live`
  WHERE target_40 IS NOT NULL;

  -- 5) Forecasts (40-day ahead) for Looker
  CREATE OR REPLACE TABLE `cocoa-prices-430315.cocoa_related.forecast_40d` AS
  SELECT
    DATE_ADD(f.date, INTERVAL 40 DAY) AS forecast_date,
    p.predicted_target_40             AS forecast_cocoa_price
  FROM ML.PREDICT(
         MODEL `cocoa-prices-430315.models.cocoa_linreg_40`,
         (SELECT *
          FROM `cocoa-prices-430315.cocoa_related.features_live`
          WHERE date >= DATE_SUB(
            (SELECT MAX(date)
             FROM `cocoa-prices-430315.cocoa_related.features_live`),
            INTERVAL 39 DAY)))
  p
  JOIN `cocoa-prices-430315.cocoa_related.features_live` f
  ON TRUE
  QUALIFY ROW_NUMBER() OVER (ORDER BY f.date DESC) <= 40
  ORDER BY forecast_date;

  -- 6) Backtest history + MAPE (for a single-value tile)
  CREATE OR REPLACE TABLE `cocoa-prices-430315.cocoa_related.pred_hist_40d` AS
  SELECT
    f.date,
    f.y,
    p.predicted_target_40 AS yhat_40
  FROM ML.PREDICT(
         MODEL `cocoa-prices-430315.models.cocoa_linreg_40`,
         (SELECT * FROM `cocoa-prices-430315.cocoa_related.features_live`)) p
  JOIN `cocoa-prices-430315.cocoa_related.features_live` f USING(date);

  CREATE OR REPLACE VIEW `cocoa-prices-430315.cocoa_related.mape_40d` AS
  WITH shifted AS (
    SELECT DATE_ADD(date, INTERVAL 40 DAY) AS asof_date, yhat_40
    FROM `cocoa-prices-430315.cocoa_related.pred_hist_40d`
  ),
  actuals AS (
    SELECT date AS asof_date, y AS actual
    FROM `cocoa-prices-430315.cocoa_related.features_live`
  )
  SELECT
    AVG(ABS(actual - yhat_40) / NULLIF(actual, 0)) * 100 AS mape_40d
  FROM shifted
  JOIN actuals USING(asof_date)
  WHERE actual IS NOT NULL;
END;
