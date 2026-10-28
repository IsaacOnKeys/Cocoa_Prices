# Timestamp provenance

Checklist item 5, branch codex/timestamp-provenance, 2026-10-28.
Status: implementation and local fixtures prepared; production deployment and
managed-storage validation pending. Python 3.11 runtime standardization is the
owner's next requirement under item 31, on a separate branch.

The daily consumer preserves observation date, producer acquisition and consumer
receipt independently. The Avro schemas and topics are unchanged: their existing
ingestion_time string carries the original producer acquisition timestamp. The
weather producer now emits explicit UTC once per acquired response, preserving
source daily dates and response metadata. Cocoa and EIA already emit aware UTC.

| Staging field | Meaning for newly accepted rows |
| --- | --- |
| date | Publisher's daily observation date, not an acquisition or receipt date. |
| producer_acquired_at | Validated producer instant, normalized to UTC; unknown/invalid is NULL. |
| producer_timestamp_raw | Exact original wire ingestion_time string, including offset or malformed content; NULL when absent. |
| consumer_received_at | UTC receipt of this consumer attempt. Replay may change it; it does not establish source freshness. |
| source_published_at | Verified publisher publication instant when available. None of the current acquisition contracts supplies one; NULL is retained. |
| timestamp_status | valid, legacy_naive_utc, missing, invalid or future. |
| ingestion_time | Compatibility field equal to producer_acquired_at for new rows across all feeds. Never replace unknown acquisition with receipt time. |

Offsets normalize before comparison. Accept second/microsecond ISO timestamps
with explicit offsets; retain their original strings. The verified old weather
writer used naive datetime.utcnow(), so only that known feed's legacy format
can normalize as UTC, with legacy_naive_utc status. Other naive strings, invalid
calendar/offset values and unsupported timestamp formats remain flagged with
NULL acquisition. A documented five-minute producer-clock tolerance bounds
future timestamps; values beyond it are flagged future. This is a technical
clock-skew allowance, not a source freshness or service-level allowance.

An old but valid acquisition stays old on replay; status valid means parseable
provenance, not fresh data. Acquisition time does not prove the publisher made
an observation available at an earlier forecast cutoff. Source-age/publication
gates remain later work. Business delivery dates use Europe/Berlin; storage
instants use UTC. Daily source dates retain their source aggregation semantics,
including NASA response time_standard metadata, without changing its query
parameters or treating a daily label as a precise UTC observation instant.

## Compatibility and historical limitations

The [nullable additive migration](../../sql/migrations/005_timestamp_provenance.sql)
adds five columns before the reader deploys. Old writers/readers keep their
existing Avro layout; no schema revision guessing or new binary layout is used.
Old consumer inserts remain compatible with nullable columns. All prior rows,
raw payloads and legacy ingestion_time values remain unchanged. New fields are
NULL for those rows; NULL timestamp_status means legacy unassessed, not verified
producer acquisition. Historical oil/weather receipt instants cannot be silently
reconstructed into lost producer instants. The item 4 quarantine/archive is
preserved, and its dated SQL remains a one-time repair, not a reusable migration.

The disabled VM Beam streaming scripts preserve historical transforms as forensic
rollback artifacts. They are not a timestamp-contract implementation or a supported
daily runtime; do not restart them to bypass managed-consumer validation. Beam
historical backfills write source-date facts without inventing acquisition times.

## Deployment and recovery

1. Capture actual serving code/configuration and staging schemas. Require code
   agreement with the preceding Git revision; preserve original sources.
2. Apply only the three additive sink changes and verify field types/modes.
3. Deploy the tested five-file consumer package, then the three-file weather
   package. Preserve runtime, service limits, trigger, routes and IAM settings.
   Schema propagation failures retry without acknowledgement.
4. Add isolated validation routes alongside the exact existing production routes.
   Keep all production subscriptions/topics unchanged. With the VM stopped, run
   the [guarded integration tool](../../scripts/diagnostics/validate_ingestion.py)
   baseline (authentication, all feeds/nulls, denied-sink retry/recovery), then
   its timestamps phase (offset/missing/invalid/legacy-naive/future/replay).
   Require actual stored producer/receipt/raw/status values and distinct replay
   receipts with identical original acquisition. Validation writes only isolated
   tables; namespace/destination guards also constrain cleanup.
5. Remove temporary routes/resources and verify the complete controlled daily
   workflow, live aware weather timestamps, all feed provenance and existing
   analytics/forecast/BI checks. Restore VM/DAG state and check Terraform drift.
6. On failure, restore the exact captured source artifacts and prior routes,
   verify serving files/settings, and investigate. Leave additive nullable
   columns and recorded evidence intact. Do not reinterpret old receipt values
   as verified acquisition or delete acknowledged observations to undo source.

Local binary fixtures cover all feeds, offset normalization, replay, missing and
invalid timestamps, future-clock bounds and sanitized logs. A real weather writer
fixture verifies one aware UTC acquisition per response with unchanged dated
observations/metadata. Managed execution and production receipts will be recorded
here after their gates pass; local tests alone are not runtime acceptance.
