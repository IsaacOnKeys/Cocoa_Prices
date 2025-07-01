DECLARE run_id STRING DEFAULT FORMAT_DATE('%Y%m%d', CURRENT_DATE());
DECLARE last_known_brent DATE DEFAULT (
  SELECT MAX(date)
  FROM `cocoa-prices-430315.cocoa_related.brent_prices`
);

EXECUTE IMMEDIATE FORMAT("""
CREATE MODEL `cocoa-prices-430315.models.cocoa_arima_xreg_%s`
OPTIONS (
  model_type                 = 'ARIMA_PLUS_XREG',
  time_series_timestamp_col  = 'date',
  time_series_data_col       = 'log_cocoa_price',
  data_frequency             = 'DAILY',
  auto_arima                 = FALSE,
  non_seasonal_order         = STRUCT(1 AS p, 1 AS d, 1 AS q),
  holiday_region             = 'CH'
)
AS
SELECT *
FROM `cocoa-prices-430315.cocoa_related.cocoa_feature_table`
WHERE log_cocoa_price IS NOT NULL
  AND date <= '%s';
""",
  run_id,
  CAST(last_known_brent AS STRING)
);
