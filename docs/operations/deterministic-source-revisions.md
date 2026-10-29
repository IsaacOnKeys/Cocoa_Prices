# Deterministic source revisions

Checklist item 17, branch `codex/deterministic-source-revisions`. The item remains
open pending the owner's valid-NULL correction policy. Items 15-16 are accepted;
their held validation and rollback evidence remain in the linked receipts.

## Decision required

An accepted acquisition can report a missing measurement for a source date that
already has a valid curated fact. The current stage-over-baseline selection
replaces that fact with NULL. Decide whether to preserve the prior valid fact
and record the new missing revision, replace it with NULL as an authoritative
correction, or withhold the release pending resolution. The recommended default
is preservation unless the supplier explicitly retracts the fact. The current
wire contracts contain missingness, not an explicit retraction operation.
No new NULL-precedence behavior has been deployed without that decision.

## Confirmed selection boundaries and remaining work

- Exact receipt generation/SHA, acquisition identity and producer-acquisition
  instant select the current source window. Consumer arrival order cannot make
  an older or unrelated acquisition authoritative. The input cutoff freezes
  that selection for retries.
- Publishers fingerprint source, observation date and business values separately
  from acquisition/transport identity. The consumer currently validates the
  identity's syntax; it should also verify that a supplied fingerprint matches
  the decoded business values, with explicit legacy compatibility.
- Staging freeze collapses equivalent transport deliveries. The SQL gate rejects
  conflicting same-feed/date/kind rows before its stage-over-baseline selector;
  no arbitrary ingestion-time winner is accepted. The canonical procedure uses
  no `ANY_VALUE`. Explicit revision/null precedence and baseline-repeat handling
  still need implementation and permutation/timestamp-tie tests.
- Historical cocoa's duplicate cleaner selects the first record in a grouped
  date. Beam provides no source-authority ordering for that selection. Replace
  it with deterministic identical-repeat/conflict handling, consistent across
  the three historical cleaners, and verify actual Python 3.11 Beam execution.
  Validation will use isolated fixtures rather than the fixed production
  `WRITE_TRUNCATE` destinations.

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
