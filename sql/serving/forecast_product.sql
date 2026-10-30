-- Display this single point with its origin, target, unit and actual issue time.
CREATE OR REPLACE VIEW `${project}.${studio}.current_forecast` AS
SELECT f.release_id,f.business_date AS forecast_origin_date,
  r.published_at AS issued_at,r.input_cutoff AS feature_cutoff,r.model_id,
  f.forecast_date AS target_date,40 AS model_horizon_days,
  DATE_DIFF(f.forecast_date,DATE(r.published_at,'Europe/Berlin'),DAY) AS days_from_issue_to_target,
  f.forecast_cocoa_price,'EUR/metric tonne' AS unit,
  COALESCE(JSON_VALUE(r.quality_results,'$.forecast_product.version'),
    'legacy_origin_40d_v1') AS forecast_product_version
FROM `${project}.${delivery}.active_release` a
JOIN `${project}.${delivery}.release_runs` r USING(release_id,business_date)
JOIN `${project}.${delivery}.forecast_40d` f USING(release_id,business_date)
WHERE a.singleton=1 AND r.status='published' AND r.published_at IS NOT NULL
  AND f.forecast_date=DATE_ADD(f.business_date,INTERVAL 40 DAY);
