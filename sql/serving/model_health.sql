CREATE OR REPLACE VIEW `${project}.${studio}.model_health` AS
WITH performance AS (
  SELECT model_id,COUNTIF(evaluation_status='evaluated') AS scored_forecasts,
    AVG(IF(evaluation_status='evaluated',ABS(forecast_cocoa_price-actual_observed_price),NULL)) AS model_mae,
    AVG(IF(evaluation_status='evaluated',ABS(persistence_price-actual_observed_price),NULL)) AS persistence_mae,
    AVG(IF(evaluation_status='evaluated',ABS(weekly_seasonal_price-actual_observed_price),NULL)) AS seasonal_mae
  FROM `${project}.${studio}.issued_forecast_evaluation` GROUP BY model_id
), serving AS (
  SELECT r.model_id,r.model_card,a.release_id FROM `${project}.${delivery}.active_release` a
  JOIN `${project}.${delivery}.release_runs` r USING(release_id) WHERE a.singleton=1
)
SELECT p.model_id AS selected_model_for_new_releases,p.review_id,p.updated_at AS selection_time,
  p.model_card AS selected_model_card,s.model_id AS serving_model_id,s.release_id AS serving_release_id,
  COALESCE(m.scored_forecasts,0) AS scored_forecasts,m.model_mae,m.persistence_mae,m.seasonal_mae,
  CASE WHEN COALESCE(m.scored_forecasts,0)<20 THEN 'provisional_insufficient_matured_forecasts'
    WHEN m.model_mae>1.05*LEAST(m.persistence_mae,m.seasonal_mae) THEN 'review_required_baseline_degradation'
    ELSE 'monitored' END AS performance_status,
  'manual_only' AS retraining_policy
FROM `${project}.${delivery}.model_policy` p LEFT JOIN serving s ON TRUE
LEFT JOIN performance m ON m.model_id=s.model_id WHERE p.singleton=1;
