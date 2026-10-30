# Deterministic source revisions

Checklist item 17, branch `codex/deterministic-source-revisions`. The owner
approved preservation of known valid measurements on October 30, 2026.
Implementation is complete; native acceptance and production cutover are in progress.

## Accepted revision policy

Only the exact generation/SHA-verified current acquisition may revise a curated
fact. Its producer-acquisition time and immutable input cutoff select the rows;
consumer arrival time and transport IDs have no source authority. A valid
non-null correction replaces the previous measurement at that observation date.
A missing measurement preserves the prior valid value at the same date. Weather
applies this independently to precipitation and soil moisture. Without a prior
value at that date, NULL remains explicit; the separate bounded calendar fill
can use an earlier dated fact only within its existing limit.

The frozen input snapshot retains both baseline and incoming missing revision,
including acquisition/observation IDs. Accepted quality metadata records
`accepted_non_null_v1`, the preservation policy, conflicting-tie refusal and the
number of preserved measurements. No retraction operation is inferred from NULL.
Current source-age checks still inspect the acquired measurements themselves;
preserving a fact cannot make a stale or entirely missing acquisition fresh.

Equivalent baseline facts and repeated transport deliveries collapse before
selection. Conflicting same-date revisions are rejected, including timestamp
ties; there is no arbitrary first record, `ANY_VALUE`, or arrival-time winner.
The selector uses explicit per-measurement precedence; independent aggregate
expressions check both curated facts and the derived calendar before publication.

The consumer verifies supplied observation hashes against the actual decoded
source/date/business values, using the producers' existing hash format. A mismatch
is durably quarantined before staging. Unidentified legacy messages retain their
NULL identities and cannot satisfy a current exact-receipt gate. No Avro layout,
producer identity algorithm or acquisition timestamp is changed.

All historical Beam cleaners emit one identical fact and retain repeated inputs
as duplicate evidence. Conflicting date facts all go to the invalid sink.
Resolution and audit output are independent of grouped iteration order. The
worker helper package is version 1.1.0 so remote installation cannot reuse the
previous helper API under its old version.

The BI quality view retains its existing fields and adds
`latest_revision_missing`, `preserved_prior_value`, `latest_observation_id` and
`latest_acquisition_id`. `source_missing` reports the incoming revision's
missingness when one exists, even when a known fact was preserved. Original value
date/age describe the retained fact. These fields follow the active release and
retained snapshots through rollback; older releases have explicit unknown
revision metadata.

## Publication-time gate correction

Review found that `frozen_provenance` incorrectly required `source_published_at`
even though the [accepted timestamp contract](timestamp-provenance.md) preserves
NULL because none of the current feeds supplies a verified publication instant.
All 43 observations in the preceding accepted cocoa/oil acquisitions retain
that legitimate NULL. The previous synthetic fixtures supplied publication
timestamps and therefore did not expose the mismatch.

The correction permits unknown supplier publication times, still requires valid
producer acquisition/consumer receipt provenance and rejects a supplied
publication time beyond the input cutoff. No observation date, acquisition time,
source-age threshold or serving value is changed. Diagnostics now exercise
truthful NULL publication times, plus an explicitly invalid future timestamp.

The independent correction is implemented in `f8422ec`, with deployed procedure
pin `6d049ba`. Native quality fixture `20261029_e630f4f9` passed all 30 cases
using NULL publication times, including a future-time rejection. Actual BQML
fixture `20261029_12d8b9ad` passed training, publication and completed retry with
truthful NULL publication times and historical expired gaps. It ran the positive
smoke path; no additional failure-injection or rollback repetition is claimed.
The preceding [bounded-fill receipt](bounded-forward-fill.md) retains those
failure/recovery proofs. All 83 focused contract/ingestion tests, changed-file
Ruff, two changed Python syntax checks and whitespace checks passed.

Installed DAG SHA256:
`c4c9f21773c9f561099c34a45a1f7a6bb9aace2e29b6977b824e45e833672c68`.
Native-verified procedure-body SHA256:
`4da1ef0d67ac92cc9e77462f2451de558ea74ad6f786b02a396128326ecfefe7`.
Only the procedure and its DAG pin change in production. The 17-field quality
view, all six serving schemas, functions, Avro, dependencies and IAM are unchanged.

Controlled run `publication_provenance__f8422ec__20261029` ran from
2026-10-29 23:00:13.807658 to 23:01:01.354649 UTC (October 30 in Berlin).
Cocoa and oil passed; NASA precipitation age 9 exceeded the unchanged 7-day
limit. SQL/summary were upstream-failed. All six serving fingerprints, the
accepted marker and legacy model remain unchanged; no successful current
production publication is claimed.

The final DAG checkpoint is unpaused/idle with legacy Beam disabled, and the
maintenance VM is restored to its prior stopped state. Four labelled disposable
datasets/models were removed. Both Terraform roots have no drift and retain
122 managed resources. The four functions and report-view definition/schema
remain unchanged. Held changed source, prior DAG/procedure and complete
native/production receipts are under
`gs://raw-historic-data/operations/2026-10-29/publication-provenance-fix/`.
The ignored parent checklist records the completion manifest generation/SHA256.
This completes the independent gate correction; item 17 stays unchecked until
the NULL policy, fingerprint verification and deterministic duplicate work pass.
