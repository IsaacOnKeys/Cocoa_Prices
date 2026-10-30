-- All products share the owner's all-or-nothing release policy.
-- Query time, not an always-on monitor, determines whether an update is overdue.
CREATE OR REPLACE VIEW `${project}.${studio}.product_delivery_status` AS
WITH clock AS (
  SELECT CURRENT_DATETIME('Europe/Berlin') AS local_now,
    CURRENT_TIMESTAMP() AS checked_at
), due_date AS (
  SELECT *,
    CASE WHEN TIME(local_now) < TIME '20:00:00'
      THEN DATE_SUB(DATE(local_now), INTERVAL 1 DAY)
      ELSE DATE(local_now) END AS candidate_date
  FROM clock
), calendar AS (
  SELECT *,
    CASE EXTRACT(DAYOFWEEK FROM candidate_date)
      WHEN 1 THEN DATE_SUB(candidate_date, INTERVAL 2 DAY)
      WHEN 7 THEN DATE_SUB(candidate_date, INTERVAL 1 DAY)
      ELSE candidate_date END AS expected_business_date
  FROM due_date
), delivery AS (
  SELECT c.checked_at,c.expected_business_date,
    r.release_id,r.business_date AS accepted_business_date,
    r.published_at AS original_publication_time,a.published_at AS activated_at,
    r.input_cutoff,r.model_id,
    CASE
      WHEN r.release_id IS NULL OR r.business_date > DATE(c.local_now) THEN 'unavailable'
      WHEN r.business_date < c.expected_business_date THEN 'missed_update'
      ELSE 'current'
    END AS delivery_status
  FROM calendar c
  LEFT JOIN `${project}.${delivery}.active_release` a ON a.singleton=1
  LEFT JOIN `${project}.${delivery}.release_runs` r
    ON r.release_id=a.release_id AND r.business_date=a.business_date AND r.status='published'
)
SELECT product,d.*,
  CASE delivery_status
    WHEN 'unavailable' THEN 'No accepted release is available; do not present product values.'
    WHEN 'missed_update' THEN 'Update overdue; showing the previous accepted release with its original dates.'
    ELSE 'Accepted release meets the latest weekday 20:00 Europe/Berlin deadline.'
  END AS status_message
FROM delivery d
CROSS JOIN UNNEST(['core_analytics','forecasts','bi_serving']) AS product;
