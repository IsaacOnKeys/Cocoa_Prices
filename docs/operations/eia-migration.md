# EIA oil-source migration: candidate and source-quality decision

Branch: `codex/eia-oil-migration`, based on `8b2861236dcc7fbe6ac28bd59f8d67960ec8928b`.
Investigation date: 2026-10-27. Status: the owner accepted the source policy
below; candidate implementation and isolated runtime validation are in progress.
Production cutover and historical warehouse replacement are not yet completed.

Production-cutover gate: automatic approval review rejected promotion into the
existing oil topic while the legacy producer remained active. The rejected
command did not execute. The temporary Airflow pause was reversed and the
original installed source is unchanged; EIA remains routed to validation.
See the concrete [cutover approval package](eia-cutover.md).

## Verified access and the earlier implementation

The existing project contains secret `EIA_OIL_API_KEY`, with an enabled version,
automatic replication and the requested app/component/source labels. Its
value was read only into process memory for API validation. It was not printed,
written to an archive, included in a command argument or added to Git.

Git history contains an EIA CSV cleaner removed in `04874a8` when the oil source
changed. The earlier input combined Brent and WTI columns; its cleaner selected
the Brent column with `csv.reader`. Commit `9d3f3d6` subsequently removed the
unused input file. No inspected commit message establishes why the source was
abandoned. The newly observed discrepancies below are not proof of the original
reason.

## Interfaces and units

The tested API route is `https://api.eia.gov/v2/petroleum/pri/spt/data/`, with
`frequency=daily`, `facets[series][]=RBRTE`, explicit start/end, ordering, offset
and length. The selected product is Europe Brent Spot Price FOB, in `$/BBL`.
The alternative `/v2/seriesid/PET.RBRTE.D` route was also checked; it showed the
same selected historical price discrepancies and did not honor the tested
start/end filters, returning the first 5,000 of 10,003 reported observations.
Neither route is assumed complete merely because it returns HTTP 200.

References: [API documentation](https://www.eia.gov/opendata/documentation.php),
[published daily series](https://www.eia.gov/dnav/pet/hist/RBRTED.htm), and
[EIA reuse guidance](https://www.eia.gov/about/copyrights_reuse.php).

## Historical comparison

The prior locally archived supplier history contains 3,004 valid dated prices.
The following new EIA acquisitions requested 2014-01-01 through 2026-10-27 and
cover published observations from 2014-01-02 through 2026-10-20:

| Comparison | EIA API | EIA published weekday table |
| --- | ---: | ---: |
| Valid observations in newly acquired history | 3,250 | 3,249 |
| Dates overlapping the prior valid history | 2,975 | 2,976 |
| Different prices on overlapping dates | 3 | 0 |
| Prior valid dates absent from the EIA interface | 29 | 28 |

The API and EIA's own published table also differ: the table has nine dates
absent from the API, the API has ten dates without a published table quote,
and three shared dates have different prices. Repeating small-window requests
reproduced the selected discrepancies; this was not a pagination-only failure.

| Date | Prior archived price / EIA published table | EIA API price |
| --- | ---: | ---: |
| 2016-12-28 | 54.95 | 53.93 |
| 2016-12-29 | 54.97 | 54.95 |
| 2018-04-16 | 72.05 | 73.18 |

Some absent dates align with market holidays, but their causes have not all
been established. Do not silently equate a missing published quote with a
fresh observation, relabel the previous supplier's data as newly acquired EIA
data, or automatically overwrite the warehouse to force equality.

Local, ignored evidence under `.build/eia-migration/`:

- `brent_oil_eia.json`: initial API investigation archive, **not approved as a
  replacement history**; SHA-256
  `acbdb51fd60135b96c8a4922f86870259c092fa8cabe277a4c60e4e1529465c9`.
- `brent_oil_eia_published.json`: newly acquired published-table observations;
  SHA-256 `4adb1bab95f29b3fc3b64bac6cf5b11f0c14c4e7fa9ec6c7b61cf22de09af087`.
- `eia_published_history.html`: source-page snapshot used to investigate the
  weekday/date mapping and the discrepancies.
- `eia_seriesid_history.json`: allowlisted public rows from the alternative API
  route, with credentials/request-query echo excluded.

The JSON files are normalized, credential-free source representations. Their
hashes do not claim byte identity with an entire HTTP response. Existing cloud
archives and warehouse data have not been replaced.

## Candidate behavior and safety boundaries

`functions/oil/main.py` now defines `publish_eia_data`. It reads the new secret,
uses bounded requests and publication waits, validates dates/series/units and
finite measurements, handles explicit nulls, checks pagination consistency,
and acquires the full requested 14-day window. Exceptions propagate with
sanitized messages; source responses and credential-bearing URLs are not logged.

Before publishing any message, the candidate reconciles valid API observations
with EIA's published weekday table over the same interval. A missing date,
different price, unexpected page format or unavailable corroboration blocks
the entire candidate batch. This introduces a deliberate published-table
availability dependency, not an undocumented fallback or automatic data repair.

The canonical oil Avro schema, generic oil topic/subscription names, staging
fields and BigQuery MERGE key remain compatible. The packager selects the new
entry point. Unit checks exercise the actual binary format through the existing
oil decoder/row mapper and test the known date/value mismatch as a rejection.

`scripts/operations/acquire_eia_history.py` can acquire a published-table archive
or explicitly request an API archive with mandatory published-table comparison.
It refuses to overwrite an archive. The API option now rejects inconsistent
historical windows before writing a candidate archive. This tool performs no
warehouse load or cloud deployment.

## Accepted source policy

The owner accepted using EIA's explicitly dated published table as the historical
authority, preserve the existing archive with its true provenance, and use the
API for ongoing acquisition only when its requested window agrees with the
published table. Treat the 28 old-only dates as absent published observations
in the new EIA history, with visible missingness/age-bounded imputation under
the service contract. Any historical warehouse/model transition must use
isolated candidate tables and measured before/after validation.

The new historical snapshot is held at
`gs://raw-historic-data/snapshots/2026-10-27/eia/brent_oil_eia.json`, generation
`1793113993070636`. Recovery of that exact generation matched the published
archive checksum above. The historical manifest and renamed EIA cleaner now
refer to this new acquisition; existing warehouse data was not overwritten.

Credential-free tests: 104 passed, including the EIA backfill, isolated-table
namespace, cross-feed incompatible-type and truncated-wire cases. Ruff passed;
24 Python syntax checks and five shell syntax checks passed before the latest
fixture additions. The packaged handler acquired six current-window records
from the real API, corroborated the interval against the real published table,
and passed their Avro through a real local Beam 2.65.0 pipeline. Publication
was captured in memory: this local run made no Pub/Sub publication or BigQuery
write. A scan found no key matches in working tracked files or the local
investigation archives. `.build/eia-migration/candidate-validation.json`
records this validation boundary.

The actual cloud route subsequently passed: deployed Eventarc trigger ->
schema-bound validation topic -> unchanged real oil Beam streaming script ->
six rows in a uniquely named temporary BigQuery table. The successful run
deleted its own validation dataset. The first run received HTTP 403 because
the trigger identity lacked Cloud Run invocation permission; a service-scoped
Terraform grant fixed it and the repeated test passed. The failed attempt and
its empty temporary dataset are recorded separately; that test-owned dataset
was subsequently verified empty and removed.

`.build/eia-migration/cloud-route-validation.json` records the passing run.
The actual serving source ZIP, generation `1793114718746253`, contains exactly
main.py, requirements.txt and oil_schema.avsc, all byte-identical to the
reviewed source and canonical schema. Candidate function state is ACTIVE and
its output environment setting is eia-oil-validation; production routing has
not yet been switched.

## Dependency checklist for the accepted migration

- [x] Separate migration branch, secret metadata verification, bounded API probes.
- [x] Earlier implementation/history investigation and full-history comparison.
- [x] Candidate producer, failure propagation and secret-safe diagnostic paths.
- [x] Existing Avro wire compatibility and consumer row-mapping unit coverage.
- [x] Accept the historical authority, missing-date policy and corroboration dependency.
- [x] Reacquire and protect the accepted history, update samples/manifest and
  rename the source-specific cleaner; validate its input format and date bounds.
- [x] Coordinate the new function identity, artifact/source generation, trigger,
  IAM, secret adoption and Terraform ownership without destroying protected
  legacy resources or double-triggering producers.
- [ ] Validate the actual producer -> Pub/Sub -> Beam -> isolated BigQuery path,
  then the intended Airflow invocation and SQL/consumer outputs.
  The isolated cloud path passed; installed Airflow/production cutover remains open.
- [ ] Update all active source-provider references, paths, diagrams, inventory,
  operations/deployment documentation and examples. Preserve dated provenance
  accurately through explicit archival/migration records; do not blind-replace
  old resource IDs or claim a new deployment that has not occurred.
- [ ] Verify deployment/rollback sequencing and cutover during a controlled window;
  retire superseded resources only after accepted end-to-end evidence.

Local unit/static checks do not establish the unchecked runtime/deployment gates.
