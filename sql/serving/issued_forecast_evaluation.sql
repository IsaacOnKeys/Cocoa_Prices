-- Actual published forecasts only; fitted-model historical rescores are excluded.
-- Retaining an as-of release key makes later revisions reproducible.
CREATE OR REPLACE VIEW `${project}.${studio}.issued_forecast_evaluation_vintages` AS
WITH issues AS (
  SELECT f.release_id,f.business_date AS origin_date,f.forecast_date AS target_date,
    f.forecast_cocoa_price,r.published_at AS issued_at,r.input_cutoff,r.model_id
  FROM `${project}.${delivery}.forecast_40d` f
  JOIN `${project}.${delivery}.release_runs` r USING(release_id,business_date)
  WHERE r.status='published' AND r.published_at IS NOT NULL
    AND JSON_VALUE(r.quality_results,'$.forecast_product.version')='single_40_day_ahead_v1'
    AND f.forecast_date=DATE_ADD(f.business_date,INTERVAL 40 DAY)
), vintages AS (
  SELECT release_id,business_date,input_cutoff FROM `${project}.${delivery}.release_runs`
  WHERE status='published' AND published_at IS NOT NULL
)
SELECT i.*,e.release_id AS evaluation_as_of_release_id,e.business_date AS evaluation_as_of_date,
  e.input_cutoff AS actual_vintage_cutoff,c.Euro_Price AS actual_observed_price,
  p.cocoa_price AS persistence_price,s.cocoa_price AS weekly_seasonal_price,
  CASE
    WHEN e.input_cutoff<i.input_cutoff THEN 'snapshot_precedes_issue'
    WHEN i.issued_at>=TIMESTAMP(i.target_date,'Europe/Berlin')
      OR i.input_cutoff>=TIMESTAMP(i.target_date,'Europe/Berlin') THEN 'issued_after_target'
    WHEN i.target_date>e.business_date THEN 'awaiting_target'
    WHEN c.Euro_Price IS NULL THEN 'no_observed_target'
    WHEN p.cocoa_price IS NULL OR s.cocoa_price IS NULL THEN 'missing_baseline'
    ELSE 'evaluated'
  END AS evaluation_status,
  'issued_forecast_out_of_sample' AS evaluation_kind
FROM issues i CROSS JOIN vintages e
LEFT JOIN `${project}.${delivery}.cocoa` c ON c.release_id=e.release_id
  AND c.business_date=e.business_date AND c.Date=i.target_date
LEFT JOIN `${project}.${delivery}.joined_daily_cocoa_oil_weather` p ON p.release_id=i.release_id
  AND p.business_date=i.origin_date AND p.date=i.origin_date
LEFT JOIN `${project}.${delivery}.joined_daily_cocoa_oil_weather` s ON s.release_id=i.release_id
  AND s.business_date=i.origin_date AND s.date=DATE_SUB(i.target_date,INTERVAL 42 DAY);

CREATE OR REPLACE VIEW `${project}.${studio}.issued_forecast_evaluation` AS
SELECT v.* FROM `${project}.${studio}.issued_forecast_evaluation_vintages` v
JOIN `${project}.${delivery}.active_release` a ON a.singleton=1
  AND a.release_id=v.evaluation_as_of_release_id;

CREATE OR REPLACE VIEW `${project}.${studio}.issued_forecast_metrics` AS
WITH methods AS (
  SELECT method FROM UNNEST(['model','persistence','weekly_seasonal']) method
), scored AS (
  SELECT v.*,m.method,CASE m.method WHEN 'model' THEN forecast_cocoa_price
    WHEN 'persistence' THEN persistence_price ELSE weekly_seasonal_price END AS predicted
  FROM methods m LEFT JOIN `${project}.${studio}.issued_forecast_evaluation` v ON TRUE
)
SELECT method,COUNT(release_id) AS issued_count,COUNTIF(evaluation_status='evaluated') AS scored_count,
  MIN(IF(evaluation_status='evaluated',origin_date,NULL)) AS first_scored_origin,
  MAX(IF(evaluation_status='evaluated',origin_date,NULL)) AS last_scored_origin,
  AVG(IF(evaluation_status='evaluated',ABS(predicted-actual_observed_price),NULL)) AS mae,
  SQRT(AVG(IF(evaluation_status='evaluated',POW(predicted-actual_observed_price,2),NULL))) AS rmse,
  100*AVG(IF(evaluation_status='evaluated',SAFE_DIVIDE(ABS(predicted-actual_observed_price),actual_observed_price),NULL)) AS mape_percent,
  CASE WHEN COUNT(release_id)=0 THEN 'no_issued_forecasts'
    WHEN COUNTIF(evaluation_status='evaluated')=0 THEN 'insufficient_matured_forecasts'
    ELSE 'evaluated' END AS evaluation_status,
  'issued_forecast_out_of_sample' AS evaluation_kind
FROM scored GROUP BY method;
