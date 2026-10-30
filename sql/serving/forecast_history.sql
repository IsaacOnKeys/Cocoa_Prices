-- Read retained accepted artifacts, never today's overwritten serving table.
CREATE OR REPLACE VIEW `${project}.${studio}.issued_forecasts` AS
SELECT f.release_id,f.business_date,r.published_at AS issued_at,
  r.input_cutoff AS feature_cutoff,r.model_id,r.code_id AS transformation_sha256,
  f.forecast_date AS target_date,
  DATE_DIFF(f.forecast_date,DATE(r.published_at,'Europe/Berlin'),DAY) AS horizon_days_from_issue,
  f.forecast_cocoa_price,'EUR/metric tonne' AS unit,'issued_snapshot' AS prediction_kind
FROM `${project}.${delivery}.forecast_40d` f
JOIN `${project}.${delivery}.release_runs` r USING(release_id,business_date)
WHERE r.status='published' AND r.published_at IS NOT NULL;

CREATE OR REPLACE VIEW `${project}.${studio}.retrospective_scores` AS
SELECT p.release_id,p.business_date,r.published_at AS release_published_at,
  r.input_cutoff,r.model_id,r.code_id AS transformation_sha256,
  p.prediction_date AS feature_origin_date,p.forecast_date AS target_date,
  p.yhat_40,p.actual,'retrospective_rescore' AS prediction_kind,
  FALSE AS is_chronological_out_of_sample
FROM `${project}.${delivery}.pred_hist_40d` p
JOIN `${project}.${delivery}.release_runs` r USING(release_id,business_date)
WHERE r.status='published' AND r.published_at IS NOT NULL;

CREATE OR REPLACE VIEW `${project}.${studio}.source_observation_vintages` AS
SELECT f.*,r.input_cutoff,r.published_at AS release_published_at,
  r.code_id AS transformation_sha256
FROM `${project}.${delivery}.frozen_inputs` f
JOIN `${project}.${delivery}.release_runs` r USING(release_id,business_date)
WHERE r.status='published' AND r.published_at IS NOT NULL;
