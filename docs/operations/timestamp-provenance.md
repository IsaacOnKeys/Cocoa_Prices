# Timestamp provenance

Checklist item 5, branch codex/timestamp-provenance, 2026-10-28.
Status: deployed; isolated managed-storage tests and the controlled production
cycle passed. Python 3.11 runtime standardization is the
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

## Deployment and acceptance receipt

Implementation: 39b935d. The nullable migration passed as job
`9576a874-f09d-4862-9427-6e09b48a77fe` with all old fields preserved.
Consumer source ZIP SHA-256:
`201e9a2961f10d700c09eef018f70e5f8a82741622fe40e3b580e4150ad0f8a9`.
After temporary route removal, serving revisions are
stage-analytics-data-00005-sos and publish-weather-data-00011-tec. Both actual
serving archives match the prepared files. Runtime, limits, trigger and IAM
settings were unchanged; only the intended source and temporary route changes
were made. Original source ZIPs are preserved in held private objects under
`gs://raw-historic-data/operations/2026-10-28/timestamp-provenance-39b935d/`.

With the VM stopped, baseline tests accepted two rows per feed, rejected
unauthenticated calls, and recovered the original delivery after denied-sink
retry. The timestamps phase accepted seven additional rows per feed across six
cases. Offset normalization, missing/invalid/future NULL acquisition, explicit
legacy weather status, raw text retention, unknown publication and distinct
replay receipts all matched. All writes were to the guarded
`ingestion_validation_20261028_69a7f103` dataset. Its temporary topics,
subscriptions, tables/dataset and function routes were removed; production
subscriptions/topics and their delivery modes never changed.

Production run `timestamp_provenance__39b935d__20261028` passed all ten tasks
from 14:46:04.122020 to 14:47:28.138266 UTC on 2026-10-28. Its procedure job was
`airflow_cocoa_daily_run_bq_pipeline_2026_10_28T14_46_02_00_00_3589bb85332d5b9b36a15635558729d0`.
Two cocoa, five oil and eight weather rows retained valid acquisition/receipt/raw
fields with zero provenance errors. Weather's eight dated observations share
one explicit aware acquisition. Maximum observed acquisition-to-receipt delays
were 1.30, 6.01 and 3.40 seconds respectively; this sample is not an achieved
latency guarantee. The existing HEAD/POST double Cocoa invocation remains item 13.
All thirteen data checks, forty unique valid forecasts and three BI source checks
passed. Rendered BI acceptance and recurring publication/failure gates remain
later items.

All 198 credential-free tests passed on both Python 3.12 and the requested Python
3.11.9 development interpreter; Ruff and Python syntax passed. Python 3.11
development dependencies passed pip check, and the existing nine pinned Beam
requirements resolved successfully (111 packages). These are not container or
Airflow runtime validation claims; that migration follows this item. At this
handoff, Airflow remains 2.10.2/provider 10.22.0 on Python 3.12.6 and the daily
consumer remains python312. The VM stays running for the immediately following
authorized Python 3.11 maintenance, with the DAG unpaused/idle and all three old
Beam startup services disabled. Final VM shutdown will be recorded with that
maintenance receipt.

Both Terraform roots have no drift (93 managed resources). The held
completed_manifest.json records the complete deployment, isolated tests,
production checks and maintenance handoff: generation 1793199377328578,
SHA-256 `275d884478ae8b3d7637ae3ed3562229f6526c2977e8998e9a92fd870dc03cf5`.
Weather's prepared three-file ZIP SHA-256 is
`6be254882462d8eea05357aed46a9ba3652e97a949924ab3f034628b8d27c8bc`.
