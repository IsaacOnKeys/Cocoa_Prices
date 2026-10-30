# Deterministic source revisions

Checklist item 17, branch `codex/deterministic-source-revisions`. The owner
approved preservation of known valid measurements on October 30, 2026.
Deployed and accepted October 30, 2026. Work stops after this item at the owner's request.

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
That receipt completed the independent gate correction before the owner approved
the revision policy. The following acceptance supersedes its deployed hashes.

## Revision-policy acceptance, October 30

Implementation `7440780`, historical fixture correction `e04549b`, conflicting
tie tests `0b2ce47`, initial source/pin promotion `3439567`, cached-receipt fix
`0d58952`, final pin `3cb7abf`.

All 516 credential-free tests, changed-file Ruff, 75 Python and five Bash syntax
checks passed. Native quality fixture `20261030_092865c2` passed 35 applicable
validity/fault cases, including conflicting acquisition timestamps/baseline facts,
preserved known measurements and rejection of NULL erasure. The shared procedure
now executes 39 named quality checks. Fixture `20261030_d29a216b` passed 18
boundary/revision/BI cases, including zero oil, independent weather corrections,
missing facts without a prior value, retained original dates and visible missing
revision/preservation flags.

Managed consumer fixture `20261030_76e69190` reconciled 15 logical cases: four
accepted and eleven quarantined, including all three supplied-hash mismatches
and real quarantine/sink denial followed by recovery. Exact production source
ZIP SHA256 is
`fe2606f536d854ef2fbc6b4ba8470669ca13246f3ac9431e33cd9c99e99d21d4`;
serving revision is `stage-analytics-data-00012-neh`. Its routes, runtime settings,
service accounts, IAM and Avro layouts are unchanged. The three producers were
not redeployed.

Full BQML fixture `20261030_1c5e9691` passed all three injected failure points,
equivalent baseline/staging repeats, frozen/completed retries, three preserved
missing measurements and guarded six-table rollback/restoration. Report
provenance followed the active marker, and all 90 expired historical measurement
rows remained NULL. An earlier fixture incorrectly counted physical deliveries
in its logical receipt; the gate rejected it, and its frozen failed request was
retained before a fresh fixture was prepared.

The rebuilt native Python 3.11.18/Beam 2.65.0 image passed actual graphs locally
and with isolated BigQuery/GCS IO. Each feed retained five valid facts. Local
invalid counts were cocoa/oil/weather 6/5/7; two cloud executions retained
12/10/14 append-only rejection records. Each run specifically verified one
identical-repeat rejection and two conflicting-date rejections per feed.
Worker `cocoa_backfill_recovery-1.1.0.tar.gz` installed offline with exact helper
bytes and the Dataflow setup option. No remote Dataflow job or production
historical overwrite was executed. The first weather conflict fixture used a
date outside the fixed historical range; its failure was preserved, then an
in-range fixture passed. The promoted stable image is
`sha256:2a557ac5927fdddd708675919acf4e588539cecb05860555efce6e59b9ceef83`;
the prior `sha256:56dc432c64b9edaf521ab25d2b7d56047f5c6070c2a751cad949e7a4a55f07ae`
remains available.

### First-call reporting correction

The first production run `source_revisions__7440780__20261030` passed every feed
and quality gate and committed release
`c337e8555a8d713fab1c5103a79862d8b033d3a419992e72764160f2eebc402b`.
Its post-commit reporting path then exceeded the remaining 1 GiB query budget;
the error surfaced in the exception handler's table update. The release remained
accepted, and the failed DAG/job outcome was preserved without relabelling it.
The parent job recorded 9,747,085 processed bytes, 1,069,547,520 billed bytes and
only 4,194,304 budget bytes remaining for a statement requiring at least 10,485,760.
[BigQuery's per-statement/table billing minimums](https://cloud.google.com/bigquery/pricing)
and [multi-statement cost rules](https://docs.cloud.google.com/bigquery/docs/multi-statement-queries)
explain why processed bytes alone did not size this cap.

The procedure now returns cached scalar receipt values after commit. Publication
and activation use the same captured timestamp. Airflow and the full atomic
diagnostic use a bounded 2 GiB maximum. Fresh fixture `20261030_550d5827` verified
model creation and successful publication in the first call, then completed
retry and guarded recovery. This closes the coverage gap in which a preceding
injected failure had already created the model before a later publication retry.

### Final production result

Run `source_revisions_final__0d58952__20261030` passed all nine tasks from
10:46:52.043298 to 10:49:48.659547 Europe/Berlin
(09:46:52.043298–09:49:48.659547 UTC). All three supplier gates and all 39 quality
checks passed. The first-attempt job completed without error and billed
1,069,547,520 bytes within its 2 GiB cap. The active release is
`851bf109355ad6d33df6bee6b548c0ac573f20049ca586edb850beff86d706ed`,
with retained model
`models.cocoa_release_851bf109355ad6d33df6bee6b548c0ac573f20049ca586edb850beff86d706ed`.
All six serving fingerprints match that release's accepted artifacts. The legacy
model and earlier accepted releases remain preserved. The 21-field quality view
and four other BI source/status views were read successfully.

Final procedure-body SHA256:
`226ee204c9c5fad275ee9295c3c97c96360895b232bdbcbca79ad3eb524672e1`.
Final deployed DAG SHA256:
`c4d00bd211995074b53a55a9c13694e5e4b818e5939009c92a4b7c115a412ff7`.
The final checkpoint has no active runs, the DAG is unpaused, legacy daily Beam
is disabled, and the maintenance VM is restored to its prior stopped state.
Thirteen labelled validation datasets/models, the temporary validation function,
routes/topics/subscriptions/bucket, prefix objects and temporary grants were
removed. Both Terraform roots have no drift and retain 122 managed resources.

Held changed source, exact prior/intermediate runtime SQL/DAG/consumer bytes,
worker distribution and native/production receipts are under
`gs://raw-historic-data/operations/2026-10-30/source-revisions/`.
The ignored parent checklist records the completion manifest generation/SHA256.
This engineering validation proves one actual accepted production cycle; it
does not establish ongoing service-level attainment, chronological forecast
skill, rendered external BI acceptance or remote Dataflow execution.
