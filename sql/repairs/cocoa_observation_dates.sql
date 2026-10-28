-- Controlled one-time repair. Render only validated table identifiers and use
-- typed query parameters. Rehearse against isolated copies before production.
-- Snapshots, quarantine and candidates must already exist. No permanent DDL
-- or model training belongs inside this DML transaction.
BEGIN TRANSACTION;

ASSERT NOT EXISTS (
  SELECT 1 FROM (
    (SELECT date, cocoa_price, ingestion_time, raw_payload, COUNT(*) n
     FROM `${ACTIVE_STAGING}` GROUP BY date, cocoa_price, ingestion_time, raw_payload
     EXCEPT DISTINCT
     SELECT date, cocoa_price, ingestion_time, raw_payload, COUNT(*) n
     FROM `${STAGING_BEFORE}` GROUP BY date, cocoa_price, ingestion_time, raw_payload)
    UNION ALL
    (SELECT date, cocoa_price, ingestion_time, raw_payload, COUNT(*) n
     FROM `${STAGING_BEFORE}` GROUP BY date, cocoa_price, ingestion_time, raw_payload
     EXCEPT DISTINCT
     SELECT date, cocoa_price, ingestion_time, raw_payload, COUNT(*) n
     FROM `${ACTIVE_STAGING}` GROUP BY date, cocoa_price, ingestion_time, raw_payload)
  )
) AS 'Staging changed since the captured preimage; abort repair';

ASSERT NOT EXISTS (
  SELECT 1 FROM (
    (SELECT Date, Euro_Price, COUNT(*) n FROM `${ACTIVE_CURATED}` GROUP BY Date, Euro_Price
     EXCEPT DISTINCT
     SELECT Date, Euro_Price, COUNT(*) n FROM `${CURATED_BEFORE}` GROUP BY Date, Euro_Price)
    UNION ALL
    (SELECT Date, Euro_Price, COUNT(*) n FROM `${CURATED_BEFORE}` GROUP BY Date, Euro_Price
     EXCEPT DISTINCT
     SELECT Date, Euro_Price, COUNT(*) n FROM `${ACTIVE_CURATED}` GROUP BY Date, Euro_Price)
  )
) AS 'Curated data changed since the captured preimage; abort repair';

ASSERT (SELECT COUNT(*) FROM `${LEGACY_ARCHIVE}`) = @legacy_rows
  AS 'Legacy quarantine does not contain the expected complete preimage';
ASSERT (SELECT COUNT(*) FROM `${STAGING_BEFORE}`) = @legacy_rows
  AS 'Unexpected staging preimage count';
ASSERT (SELECT COUNT(*) FROM `${CANDIDATE_STAGING}`) = @dated_rows
  AS 'Unexpected publisher candidate count';
ASSERT NOT EXISTS (
  SELECT date FROM `${CANDIDATE_STAGING}` GROUP BY date HAVING COUNT(*) != 1
) AS 'Publisher candidate contains duplicate dates';
ASSERT NOT EXISTS (
  SELECT 1 FROM `${CANDIDATE_STAGING}`
  WHERE date IS NULL OR date < @interval_start OR date > @interval_end
     OR cocoa_price IS NULL OR IS_NAN(cocoa_price) OR IS_INF(cocoa_price) OR cocoa_price <= 0
     OR JSON_VALUE(raw_payload, '$.date_basis') IS DISTINCT FROM 'publisher_dated_history'
     OR JSON_VALUE(raw_payload, '$.date') IS DISTINCT FROM CAST(date AS STRING)
) AS 'Invalid publisher candidate';

-- Legacy rows remain unchanged in snapshots, durable quarantine and held GCS
-- evidence. Their original acquisition-date labels must not re-enter MERGE.
DELETE FROM `${ACTIVE_STAGING}` AS T
WHERE T.date BETWEEN @interval_start AND @interval_end
  AND JSON_VALUE(T.raw_payload, '$.date_basis') IS NULL
  AND EXISTS (
    SELECT 1 FROM `${STAGING_BEFORE}` S
    WHERE T.date = S.date
      AND T.cocoa_price = S.cocoa_price
      AND T.ingestion_time IS NOT DISTINCT FROM S.ingestion_time
      AND T.raw_payload IS NOT DISTINCT FROM S.raw_payload
  );
INSERT INTO `${ACTIVE_STAGING}` (date, cocoa_price, ingestion_time, raw_payload)
SELECT date, cocoa_price, ingestion_time, raw_payload FROM `${CANDIDATE_STAGING}`;

-- Rewrite only the affected date interval; earlier bootstrap facts are kept.
DELETE FROM `${ACTIVE_CURATED}` WHERE Date BETWEEN @interval_start AND @interval_end;
INSERT INTO `${ACTIVE_CURATED}` (Date, Euro_Price)
SELECT Date, Euro_Price FROM `${CANDIDATE_CURATED}`
WHERE Date BETWEEN @interval_start AND @interval_end;

ASSERT (SELECT COUNT(*) FROM `${ACTIVE_STAGING}`) = @dated_rows
  AS 'Unexpected repaired staging count';
ASSERT (SELECT COUNT(*) FROM `${ACTIVE_CURATED}`) = @curated_rows
  AS 'Unexpected repaired curated count';
ASSERT NOT EXISTS (
  SELECT 1 FROM (
    (SELECT Date, Euro_Price FROM `${ACTIVE_CURATED}`
     EXCEPT DISTINCT SELECT Date, Euro_Price FROM `${CANDIDATE_CURATED}`)
    UNION ALL
    (SELECT Date, Euro_Price FROM `${CANDIDATE_CURATED}`
     EXCEPT DISTINCT SELECT Date, Euro_Price FROM `${ACTIVE_CURATED}`)
  )
) AS 'Repaired curated data does not match the reviewed candidate';

COMMIT TRANSACTION;
