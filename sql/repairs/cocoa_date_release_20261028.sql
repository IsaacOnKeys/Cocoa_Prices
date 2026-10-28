-- One-time cocoa observation-date release, validated for 2026-10-28.

-- DO NOT reuse as a daily job. Exact preimages and candidate hashes are required.

-- Backups/model/candidate validation: docs/operations/cocoa-date-correction.md.

-- The copied candidate model must be verified before this DML transaction.

BEGIN TRANSACTION;
ASSERT CURRENT_DATE('Europe/Berlin') = DATE '2026-10-28' AS 'Candidate release is stale';

ASSERT NOT EXISTS (SELECT 1 FROM ((SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.brent_prices` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.brent_before` T GROUP BY row_value) UNION ALL (SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.brent_before` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.brent_prices` T GROUP BY row_value))) AS 'Production brent_prices changed since backup';

ASSERT NOT EXISTS (SELECT 1 FROM ((SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.precipitation` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.weather_before` T GROUP BY row_value) UNION ALL (SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.weather_before` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.precipitation` T GROUP BY row_value))) AS 'Production precipitation changed since backup';

ASSERT NOT EXISTS (SELECT 1 FROM ((SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.joined_daily_cocoa_oil_weather` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.joined_before` T GROUP BY row_value) UNION ALL (SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.joined_before` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.joined_daily_cocoa_oil_weather` T GROUP BY row_value))) AS 'Production joined_daily_cocoa_oil_weather changed since backup';

ASSERT NOT EXISTS (SELECT 1 FROM ((SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.forecast_40d` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.forecast_before` T GROUP BY row_value) UNION ALL (SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.forecast_before` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.forecast_40d` T GROUP BY row_value))) AS 'Production forecast_40d changed since backup';

ASSERT NOT EXISTS (SELECT 1 FROM ((SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.pred_hist_40d` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.predictions_before` T GROUP BY row_value) UNION ALL (SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.predictions_before` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.pred_hist_40d` T GROUP BY row_value))) AS 'Production pred_hist_40d changed since backup';

ASSERT (SELECT TO_HEX(SHA256(STRING_AGG(TO_JSON_STRING(T), '\n' ORDER BY TO_JSON_STRING(T)))) FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.candidate_staging` T) = 'c754f1a1d7b88ca4d5ebd9e18dd785079d0a00654f7c9252f915b802b292e3e4' AS 'Validated candidate candidate_staging changed';

ASSERT (SELECT TO_HEX(SHA256(STRING_AGG(TO_JSON_STRING(T), '\n' ORDER BY TO_JSON_STRING(T)))) FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.candidate_curated` T) = 'c9049fa78421aab4c58589a35b2c4e779232ceebab9f582e99f12b6499acf00f' AS 'Validated candidate candidate_curated changed';

ASSERT (SELECT TO_HEX(SHA256(STRING_AGG(TO_JSON_STRING(T), '\n' ORDER BY TO_JSON_STRING(T)))) FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.legacy_observations` T) = '67b0fcaa04591e462feda6e121432c778e06e66c3abb10e726b4b17b45fb01a4' AS 'Validated candidate legacy_observations changed';

ASSERT (SELECT TO_HEX(SHA256(STRING_AGG(TO_JSON_STRING(T), '\n' ORDER BY TO_JSON_STRING(T)))) FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.brent_prices` T) = '4f5b919adfb61e710ffa722c579a2211b2d5945a953504fe4b056696be6ef967' AS 'Validated candidate brent_prices changed';

ASSERT (SELECT TO_HEX(SHA256(STRING_AGG(TO_JSON_STRING(T), '\n' ORDER BY TO_JSON_STRING(T)))) FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.precipitation` T) = '590df24fd293902e75a0f134db1f01390ee5c12abd43bbe2a222508acc147850' AS 'Validated candidate precipitation changed';

ASSERT (SELECT TO_HEX(SHA256(STRING_AGG(TO_JSON_STRING(T), '\n' ORDER BY TO_JSON_STRING(T)))) FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.joined_daily_cocoa_oil_weather` T) = 'a32f6a14e4e1e288cec6217bdf3cc5b38e37119aa9d48aa105e4cb5e416611a8' AS 'Validated candidate joined_daily_cocoa_oil_weather changed';

ASSERT (SELECT TO_HEX(SHA256(STRING_AGG(TO_JSON_STRING(T), '\n' ORDER BY TO_JSON_STRING(T)))) FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.forecast_40d` T) = 'bce4b69b299ebc264f0117eee26cb4c03dab4272bfaa9c1a1564f2752cb850ea' AS 'Validated candidate forecast_40d changed';

ASSERT (SELECT TO_HEX(SHA256(STRING_AGG(TO_JSON_STRING(T), '\n' ORDER BY TO_JSON_STRING(T)))) FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.pred_hist_40d` T) = 'cf8388f17deab0fc7c8568478f89bcfbcacbec7cadfc37946868d19bb1443e3a' AS 'Validated candidate pred_hist_40d changed';

ASSERT NOT EXISTS (SELECT 1 FROM ((SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.brent_prices` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.brent_before` T GROUP BY row_value) UNION ALL (SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.brent_before` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.brent_prices` T GROUP BY row_value))) AS 'Unrelated source data changed in candidate';

ASSERT NOT EXISTS (SELECT 1 FROM ((SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.precipitation` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.weather_before` T GROUP BY row_value) UNION ALL (SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.weather_before` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.precipitation` T GROUP BY row_value))) AS 'Unrelated source data changed in candidate';

ASSERT (SELECT COUNT(*) = 40 AND COUNT(DISTINCT forecast_date) = 40
 AND MIN(forecast_date) = DATE '2026-10-29' AND MAX(forecast_date) = DATE '2026-12-07'
 AND COUNTIF(forecast_cocoa_price IS NULL OR IS_NAN(forecast_cocoa_price)
 OR IS_INF(forecast_cocoa_price) OR forecast_cocoa_price <= 0) = 0
 FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.forecast_40d`) AS 'Candidate forecast is incomplete or invalid';

-- Controlled one-time repair. Render only validated table identifiers and use
-- typed query parameters. Rehearse against isolated copies before production.
-- Snapshots, quarantine and candidates must already exist. No permanent DDL
-- or model training belongs inside this DML transaction.


ASSERT NOT EXISTS (
  SELECT 1 FROM (
    (SELECT date, cocoa_price, ingestion_time, raw_payload, COUNT(*) n
     FROM `cocoa-prices-430315.stream_staging.cocoa_prices` GROUP BY date, cocoa_price, ingestion_time, raw_payload
     EXCEPT DISTINCT
     SELECT date, cocoa_price, ingestion_time, raw_payload, COUNT(*) n
     FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.staging_before` GROUP BY date, cocoa_price, ingestion_time, raw_payload)
    UNION ALL
    (SELECT date, cocoa_price, ingestion_time, raw_payload, COUNT(*) n
     FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.staging_before` GROUP BY date, cocoa_price, ingestion_time, raw_payload
     EXCEPT DISTINCT
     SELECT date, cocoa_price, ingestion_time, raw_payload, COUNT(*) n
     FROM `cocoa-prices-430315.stream_staging.cocoa_prices` GROUP BY date, cocoa_price, ingestion_time, raw_payload)
  )
) AS 'Staging changed since the captured preimage; abort repair';

ASSERT NOT EXISTS (
  SELECT 1 FROM (
    (SELECT Date, Euro_Price, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.cocoa` GROUP BY Date, Euro_Price
     EXCEPT DISTINCT
     SELECT Date, Euro_Price, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.curated_before` GROUP BY Date, Euro_Price)
    UNION ALL
    (SELECT Date, Euro_Price, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.curated_before` GROUP BY Date, Euro_Price
     EXCEPT DISTINCT
     SELECT Date, Euro_Price, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.cocoa` GROUP BY Date, Euro_Price)
  )
) AS 'Curated data changed since the captured preimage; abort repair';

ASSERT (SELECT COUNT(*) FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.legacy_observations`) = @legacy_rows
  AS 'Legacy quarantine does not contain the expected complete preimage';
ASSERT (SELECT COUNT(*) FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.staging_before`) = @legacy_rows
  AS 'Unexpected staging preimage count';
ASSERT (SELECT COUNT(*) FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.candidate_staging`) = @dated_rows
  AS 'Unexpected publisher candidate count';
ASSERT NOT EXISTS (
  SELECT date FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.candidate_staging` GROUP BY date HAVING COUNT(*) != 1
) AS 'Publisher candidate contains duplicate dates';
ASSERT NOT EXISTS (
  SELECT 1 FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.candidate_staging`
  WHERE date IS NULL OR date < @interval_start OR date > @interval_end
     OR cocoa_price IS NULL OR IS_NAN(cocoa_price) OR IS_INF(cocoa_price) OR cocoa_price <= 0
     OR JSON_VALUE(raw_payload, '$.date_basis') IS DISTINCT FROM 'publisher_dated_history'
     OR JSON_VALUE(raw_payload, '$.date') IS DISTINCT FROM CAST(date AS STRING)
) AS 'Invalid publisher candidate';

-- Legacy rows remain unchanged in snapshots, durable quarantine and held GCS
-- evidence. Their original acquisition-date labels must not re-enter MERGE.
DELETE FROM `cocoa-prices-430315.stream_staging.cocoa_prices` AS T
WHERE T.date BETWEEN @interval_start AND @interval_end
  AND JSON_VALUE(T.raw_payload, '$.date_basis') IS NULL
  AND EXISTS (
    SELECT 1 FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.staging_before` S
    WHERE T.date = S.date
      AND T.cocoa_price = S.cocoa_price
      AND T.ingestion_time IS NOT DISTINCT FROM S.ingestion_time
      AND T.raw_payload IS NOT DISTINCT FROM S.raw_payload
  );
INSERT INTO `cocoa-prices-430315.stream_staging.cocoa_prices` (date, cocoa_price, ingestion_time, raw_payload)
SELECT date, cocoa_price, ingestion_time, raw_payload FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.candidate_staging`;

-- Rewrite only the affected date interval; earlier bootstrap facts are kept.
DELETE FROM `cocoa-prices-430315.cocoa_related.cocoa` WHERE Date BETWEEN @interval_start AND @interval_end;
INSERT INTO `cocoa-prices-430315.cocoa_related.cocoa` (Date, Euro_Price)
SELECT Date, Euro_Price FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.candidate_curated`
WHERE Date BETWEEN @interval_start AND @interval_end;

ASSERT (SELECT COUNT(*) FROM `cocoa-prices-430315.stream_staging.cocoa_prices`) = @dated_rows
  AS 'Unexpected repaired staging count';
ASSERT (SELECT COUNT(*) FROM `cocoa-prices-430315.cocoa_related.cocoa`) = @curated_rows
  AS 'Unexpected repaired curated count';
ASSERT NOT EXISTS (
  SELECT 1 FROM (
    (SELECT Date, Euro_Price FROM `cocoa-prices-430315.cocoa_related.cocoa`
     EXCEPT DISTINCT SELECT Date, Euro_Price FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.candidate_curated`)
    UNION ALL
    (SELECT Date, Euro_Price FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.candidate_curated`
     EXCEPT DISTINCT SELECT Date, Euro_Price FROM `cocoa-prices-430315.cocoa_related.cocoa`)
  )
) AS 'Repaired curated data does not match the reviewed candidate';




DELETE FROM `cocoa-prices-430315.cocoa_related.joined_daily_cocoa_oil_weather` WHERE TRUE;
INSERT INTO `cocoa-prices-430315.cocoa_related.joined_daily_cocoa_oil_weather` SELECT * FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.joined_daily_cocoa_oil_weather`;

ASSERT NOT EXISTS (SELECT 1 FROM ((SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.joined_daily_cocoa_oil_weather` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.joined_daily_cocoa_oil_weather` T GROUP BY row_value) UNION ALL (SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.joined_daily_cocoa_oil_weather` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.joined_daily_cocoa_oil_weather` T GROUP BY row_value))) AS 'Published joined_daily_cocoa_oil_weather differs from candidate';

DELETE FROM `cocoa-prices-430315.cocoa_related.forecast_40d` WHERE TRUE;
INSERT INTO `cocoa-prices-430315.cocoa_related.forecast_40d` SELECT * FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.forecast_40d`;

ASSERT NOT EXISTS (SELECT 1 FROM ((SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.forecast_40d` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.forecast_40d` T GROUP BY row_value) UNION ALL (SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.forecast_40d` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.forecast_40d` T GROUP BY row_value))) AS 'Published forecast_40d differs from candidate';

DELETE FROM `cocoa-prices-430315.cocoa_related.pred_hist_40d` WHERE TRUE;
INSERT INTO `cocoa-prices-430315.cocoa_related.pred_hist_40d` SELECT * FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.pred_hist_40d`;

ASSERT NOT EXISTS (SELECT 1 FROM ((SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.pred_hist_40d` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.pred_hist_40d` T GROUP BY row_value) UNION ALL (SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4.pred_hist_40d` T GROUP BY row_value EXCEPT DISTINCT SELECT TO_JSON_STRING(T) row_value, COUNT(*) n FROM `cocoa-prices-430315.cocoa_related.pred_hist_40d` T GROUP BY row_value))) AS 'Published pred_hist_40d differs from candidate';

COMMIT TRANSACTION;
