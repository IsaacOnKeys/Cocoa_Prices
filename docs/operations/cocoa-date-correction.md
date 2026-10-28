# Cocoa observation-date correction

Checklist item 4; branch codex/cocoa-observation-dates, 2026-10-28.
Status: daily producer fix tested against fixtures and the live publisher;
historical backup/candidate preparation and isolated transaction rehearsal in
progress. Production source/data have not yet been changed for this item.

## Defect and corrected contract

The actual deployed producer matches the prior repository code. It parses
ICCO's observation date, discards it, and assigns the UTC acquisition date to
both the wire date and raw_payload.date. Thus a lagged quote becomes a false
new daily fact. The raw payload alone cannot recover the discarded date.

The fix returns the parsed date with its EUR/tonne price and preserves that
date in Avro, raw evidence and the HTTP response. ingestion_time remains the
aware UTC acquisition instant. New raw payloads include
date_basis=source_observation so future audits can distinguish corrected
acquisitions from legacy ones. The Avro layout and staging columns are unchanged.

Real-source probe, with publication intercepted in memory: observation
2026-10-27 at EUR 5,121.21/tonne, acquired October 28. No production message
or warehouse write was made by that probe. Regression fixtures cover lagged
dates, unordered source rows, invalid/missing data, request failure and parser
fallback. Existing dependencies/schema match the deployed baseline; only the
producer code changes. Beautiful Soup is added to development requirements
for real-parser fixtures, not to a new production dependency family.

## Historical impact and source evidence

Staging contains 707 records across 345 wire dates, acquired from
2025-09-28 through 2026-10-27. Retained execution logs correlate fifty records
by execution/instance/revision identity, exact acquisition timestamp, price
and raw-payload fingerprint. All fifty have the wrong wire date. The other
657 original observation dates cannot be recovered from retained logs; do
not infer them from matching prices or shift every record by a fixed lag.

ICCO's [public statistics table](https://www.icco.org/statistics/) exposes
ordinary paginated daily prices with an offered 100-row page size. A complete
read captured 550 source rows: 532 uniquely dated finite positive quotes and
eighteen entirely empty undated rows, with no conflicting dates. The accepted
coverage is 2024-10-01 through 2026-10-27. The latest two homepage quotes agree
with those dated observations. All 258 overlapping dates in the protected
bootstrap archive match exactly, as do the fifty log-recovered observations.

The publisher added October 26-27 during the investigation. The earlier
acquisition ending October 23 is retained as investigation evidence; the
accepted repair uses the refreshed, corroborated acquisition. Availability
then is not inferred from today's published history. Current historical
vintage does not establish what was available at a past forecast cutoff.

## Reviewed reconstruction

Preserve all legacy records unchanged in cloud snapshots, held/checksummed GCS
objects and a separate quarantine table. Recovered dates are additional
evidence, not replacements for the original payload. Rebuild the affected
active interval from explicitly dated ICCO observations. Source dates absent
from the accepted publisher acquisition remain absent, without fabricated
quotes or market-closure explanations.

| Measure | Before | Candidate |
| --- | ---: | ---: |
| Active staging rows | 707 legacy acquisitions | 277 dated publisher observations |
| Preserved legacy/quarantine rows | 707 | 707, unchanged |
| Curated cocoa rows | 3,360 | 3,292 |
| Curated prices corrected on shared dates | — | 253 |
| Unsupported legacy date labels removed from active curated data | — | 91 |
| Published dates previously missing from curated data | — | 23 |

The affected interval is 2025-09-28 through 2026-10-27. Earlier bootstrap facts
remain unchanged. The candidate source acquisition time is retained separately
from its observation dates, with date_basis=publisher_dated_history and an
exact held acquisition reference. This is new authoritative acquisition,
not a claim to have recovered the unknown vintage of 657 legacy messages.

## Deployment, safety gates and recovery

1. Preserve/verify the actual prior producer source and configuration. Its
   source ZIP SHA-256 is
   `eb1c10dbe5af8f4f1cf2f5f65e1e2d98ed06c5b34253e42f5464c7f5af1710ab`.
2. Protect raw source, legacy rows, correlation evidence and the candidate plan
   in private held GCS objects. Preserve exact-generation recovery hashes.
3. Snapshot staging, curated source tables, joined data, forecasts and historical
   predictions; copy the prior model and verify its prediction matches the
   saved forecast. Store them in an isolated dated recovery dataset.
4. Rehearse the exact [DML transaction](../../sql/repairs/cocoa_observation_dates.sql)
   on isolated copies. Inject a preimage change and require an abort with no
   partial mutations, then verify the valid candidate's counts and values.
5. Pause the idle DAG in a controlled window. Deploy only the tested producer
   source, verify its serving files and unchanged non-source settings, then
   apply the rehearsed transaction. It compares complete preimage multisets,
   preserves quarantine, replaces only captured legacy staging rows and
   rewrites only the affected curated interval. No WRITE_TRUNCATE bootstrap
   or permanent DDL inside a transaction is used.
6. Require a controlled full daily cycle and source/feature/forecast/BI checks
   before accepting the maintenance result. On failure, restore the exact
   saved products/model from the recovery dataset and investigate; source
   rollback cannot reverse acknowledged messages by itself.
7. Restore the prior pause/VM state and normal schedules. Keep the dated
   recovery artifacts and archive references; generic backfill, versioned
   releases, chronological evaluation and recurring quality gates remain their
   separate checklist items.

The pure [candidate planner](../../scripts/operations/plan_cocoa_date_repair.py)
makes no cloud changes. It rejects incompatible units, missing coverage,
conflicting quotes, future dates, pre-corrected staging and duplicate curated
keys. SQL rendering permits only the fixed production pair or matching
isolated rehearsal destinations. Private execution evidence is under
.build/cocoa-date/; the recovery manifest records actual dataset/object IDs.
