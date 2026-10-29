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
  No production backfill is authorized by a diagnostic or these notes.

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
Native acceptance and the controlled deployment receipt are being finalized.
