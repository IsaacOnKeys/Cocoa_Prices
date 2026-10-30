-- One approved selection plus append-only human review history; existing dataset/IAM.
ALTER TABLE `${project}.${delivery}.release_runs` ADD COLUMN IF NOT EXISTS model_card JSON;
CREATE TABLE IF NOT EXISTS `${project}.${delivery}.model_policy` (
  singleton INT64 NOT NULL,model_id STRING NOT NULL,review_id STRING NOT NULL,
  model_card JSON NOT NULL,updated_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS `${project}.${delivery}.model_reviews` (
  review_id STRING NOT NULL,previous_review_id STRING,model_id STRING NOT NULL,
  reviewer STRING NOT NULL,reason STRING NOT NULL,model_card JSON NOT NULL,
  model_card_sha256 STRING NOT NULL,
  reviewed_at TIMESTAMP NOT NULL
);
