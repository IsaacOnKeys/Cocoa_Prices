-- Operator-only selection. It affects new releases, not already issued predictions.
CREATE OR REPLACE PROCEDURE `${project}.${delivery}.select_reviewed_model`(
  p_expected STRING,p_review STRING,p_model STRING,p_reviewer STRING,p_reason STRING,p_card STRING
)
BEGIN
  DECLARE card JSON DEFAULT PARSE_JSON(p_card);
  DECLARE in_transaction BOOL DEFAULT FALSE;
  BEGIN
  ASSERT REGEXP_CONTAINS(p_model,r'^${project}\.${models}\.[A-Za-z_][A-Za-z0-9_]*$')
    AND REGEXP_CONTAINS(p_review,r'^[a-f0-9]{64}$')
    AND LENGTH(p_reviewer) BETWEEN 1 AND 128 AND LENGTH(p_reason) BETWEEN 1 AND 1024
    AND JSON_VALUE(card,'$.model_id')=p_model
    AND JSON_VALUE(card,'$.review_id')=p_review
    AND JSON_VALUE(card,'$.retraining_policy')='manual_only'
    AS 'Invalid reviewed model selection';
  ASSERT JSON_VALUE(card,'$.eligibility')='retained_accepted_baseline' OR (
    JSON_VALUE(card,'$.gate_passed')='true'
    AND SAFE_CAST(JSON_VALUE(card,'$.training_rows') AS INT64)>=365
    AND SAFE_CAST(JSON_VALUE(card,'$.evaluation.holdout_rows') AS INT64)>=20
    AND SAFE_CAST(JSON_VALUE(card,'$.evaluation.invalid_predictions') AS INT64)=0
    AND SAFE_CAST(JSON_VALUE(card,'$.evaluation.model_mae') AS FLOAT64)<=1.05*LEAST(
      SAFE_CAST(JSON_VALUE(card,'$.evaluation.persistence_mae') AS FLOAT64),
      SAFE_CAST(JSON_VALUE(card,'$.evaluation.seasonal_mae') AS FLOAT64))
  ) AS 'Candidate is not eligible for reviewed promotion';
  IF EXISTS (SELECT 1 FROM `${project}.${delivery}.model_reviews` WHERE review_id=p_review) THEN
    ASSERT (SELECT COUNT(*) FROM `${project}.${delivery}.model_reviews`
      WHERE review_id=p_review AND previous_review_id=p_expected AND model_id=p_model
        AND reviewer=p_reviewer AND reason=p_reason
        AND model_card_sha256=LOWER(TO_HEX(SHA256(p_card))))=1
      AS 'Review identity already belongs to a different decision';
  ELSE
    BEGIN TRANSACTION;
    SET in_transaction = TRUE;
    UPDATE `${project}.${delivery}.model_policy`
      SET model_id=p_model,review_id=p_review,model_card=card,updated_at=CURRENT_TIMESTAMP()
      WHERE singleton=1 AND review_id=p_expected;
    ASSERT @@row_count=1 AS 'Model selection changed; review the current selection again';
    INSERT `${project}.${delivery}.model_reviews`
      VALUES(p_review,p_expected,p_model,p_reviewer,p_reason,card,LOWER(TO_HEX(SHA256(p_card))),CURRENT_TIMESTAMP());
    COMMIT TRANSACTION;
    SET in_transaction = FALSE;
  END IF;
  SELECT * FROM `${project}.${delivery}.model_policy` WHERE singleton=1;
  EXCEPTION WHEN ERROR THEN
    IF in_transaction THEN ROLLBACK TRANSACTION; END IF;
    RAISE;
  END;
END;
