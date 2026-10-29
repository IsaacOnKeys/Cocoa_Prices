-- Retired direct-publishing entrypoint. Recovery source remains in Git history
-- and the generation-pinned pre-cutover archive. Render before deployment.
CREATE OR REPLACE PROCEDURE `${project}.${curated}.run_daily_pipeline`()
BEGIN
  RAISE USING MESSAGE = 'Direct publication is retired. Use ${project}.${delivery}.run_daily_pipeline with an explicit logical run and accepted source receipts.';
END;
