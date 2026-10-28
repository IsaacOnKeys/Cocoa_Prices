# Cocoa observation-date correction

Checklist item 4; branch codex/cocoa-observation-dates, 2026-10-28.
Status: producer fix deployed and complete historical/analytics/forecast
repair published after isolated validation and rollback rehearsal. The controlled
production workflow and final warehouse acceptance checks passed.

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
4. Rehearse the core [DML transaction](../../sql/repairs/cocoa_observation_dates.sql)
   on isolated copies. A changed preimage must abort without mutations. Build
   the complete candidate by executing the actual deployed procedure body
   against frozen, corrected inputs in the recovery namespace. Freeze its
   business date to 2026-10-28; require all thirteen feature/output checks,
   forty unique valid forecasts, and complete BI source views.
5. Rehearse the concrete [complete-release transaction](../../sql/repairs/cocoa_date_release_20261028.sql)
   against separate copies. An injected post-write failure must roll back
   every affected table. Pause the idle DAG; deploy/verify only producer
   source, retain unchanged non-source settings, copy and verify the candidate
   model, then publish corrected cocoa, joined data and both prediction tables
   in one DML transaction. Exact multiset preimages, candidate fingerprints,
   date/count/domain guards and a same-day expiry prevent stale application.
   Oil/weather facts must match their backups and are not rewritten. Existing
   feature/metric/BI view definitions remain unchanged. Restore the prior model
   if the DML job definitively fails; reconcile an ambiguous job outcome first.
6. Require a controlled full daily cycle and source/feature/forecast/BI checks
   before accepting the result. Retain the validated candidate as the complete
   maintenance recovery release; the pre-repair products/model remain separately
   available for forensic recovery. Source rollback alone cannot reverse
   acknowledged messages. This one-time repair does not change the normal
   DAG's publication behavior; recurring atomic releases remain item 20.
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

## Deployment and repair receipt

- Source changes: b652c5a and f2aa51a. Serving revision:
  publish-price-00017-yoz; prepared three-file ZIP SHA-256:
  `04c422b36aeeb05c7cd0ceca0db9e7b93260ffec2268ac5cefd313255fa2b7f0`.
  Serving files match; runtime, trigger and invocation policy are unchanged.
- Recovery dataset:
  `cocoa-prices-430315.cocoa_date_repair_20261028_569854f4`.
  It contains read-only source/output snapshots, verified model backup,
  unchanged legacy quarantine, accepted candidates and isolated rehearsals.
  These are retained application recovery artifacts outside Terraform ownership.
- Private held evidence prefix:
  `gs://raw-historic-data/repairs/2026-10-28/cocoa_date_repair_20261028_569854f4/`.
  prepared_manifest.json records initial snapshots/evidence;
  validated_bundle_manifest.json records validated candidates, fingerprints,
  job IDs and successful failure-injection rehearsal. release_transaction.sql
  is the exact published SQL. Object generations/hashes are recorded in the
  private execution state; original archives are preserved.
- Atomic publication job: `b389dcad-4ebf-4a4d-9522-ed6591073d78`.
- Credential-free regression suite: 159 passed. Parser fixtures produce seven
  existing Beautiful Soup/lxml deprecation warnings. No hosted CI is claimed.

Checks on BI source views establish warehouse output integrity; rendered
report behavior and user access still require their later BI acceptance item.
The model retains its existing evaluation semantics; corrected dates do not
turn retrospective rescoring into an honest chronological backtest.

## Production acceptance

The controlled run `cocoa_date_correction__f2aa51a__20261028` passed all ten tasks
from 09:54:45.853444 to 09:55:57.882770 UTC on 2026-10-28. Its procedure job was
`airflow_cocoa_daily_run_bq_pipeline_2026_10_28T09_54_45_00_00_4135e95b0f993a2025d853cbed9e7ccb`.
The normal HTTP HEAD probe and POST still produce two cocoa acquisitions; fixing
that invocation behavior remains item 13. Both new acquisitions retained the
October 27 source date on October 28, with matching raw date/price evidence.

Final active staging has 277 publisher-history rows plus two corrected live
acquisitions and no unmarked legacy rows. Curated cocoa has 3,292 unique dated
facts and matches the accepted candidate exactly. All earlier bootstrap facts
remain unchanged. Quarantine retains all 707 legacy records unchanged, fifty
with additional recovered-date evidence and 657 with unknown original source
dates.

All thirteen feature/output checks passed. Joined/features/prediction-history
outputs each contain 4,683 unique calendar rows through October 28. Forecasts
contain forty unique finite positive values for October 29 through December 7.
The three BI source checks have no missing outputs; all feed freshness views
reflect the controlled acquisitions. These checks establish structural and
numeric validity, without claiming predictive accuracy or rendered-chart
acceptance. The normal daily DAG retains the known permissive failure handling
and non-atomic publication behavior scheduled for later checklist items.

The DAG is restored unpaused and the Airflow VM is verified TERMINATED outside
its operating window. All three old Beam services remain disabled/inactive,
including after a real VM restart. Both Terraform roots have no drift (93
managed resources). The held completed_manifest.json records final production
checks and recovery state: generation 1793181569676469, SHA-256
`f015f53eff1d40a29f180ae7e18d9a05c36572cf4b43888750e6f9b15942b4ec`.
