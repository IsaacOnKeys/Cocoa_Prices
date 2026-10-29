# Invalid-input quarantine and sink recovery

Checklist item 11, branch `codex/invalid-record-quarantine`.

The daily consumer stores an immutable, checksummed copy of each failed push
envelope under `gs://raw-historic-data/quarantine/v1/`. Evidence includes the
original body, completeness flag, input checksum, acquisition/revision/delivery
and message identities when available, category, sanitized error codes/status,
receipt time and disposition. Headers, arbitrary exceptions and sink response
bodies are excluded. Upload uses create-only preconditions; the exact generation
is read back and its SHA256 verified before an invalid input can receive 204.
The structured log distinguishes ingestion_accepted, ingestion_quarantined and
ingestion_retry, including the durable reference when persistence succeeded.

Malformed complete envelopes/Avro, invalid canonical or future observation dates,
nonfinite values, nonpositive cocoa prices, negative precipitation and soil
moisture outside [0,1] are quarantined. Nullable measurements remain explicit
source missingness. Finite oil prices do not acquire a new sign restriction.
The existing timestamp provenance contract is unchanged: unknown/invalid source
times remain NULL with status, and required-feed publication gates reject invalid
provenance. Quarantine does not manufacture current acquisition timestamps.

Permanent BigQuery row rejection with invalid/stopped reasons is quarantined.
HTTP failures, ambiguous writes, transient/unknown row errors and route/configuration
failures retain diagnostic evidence and return 503 for recovery. A missing,
denied or unverifiable quarantine write also returns 503. Only complete evidence
can establish quarantine; oversized truncated evidence always remains retrying.
The 16 MiB envelope bound accommodates base64 expansion of Pub/Sub's maximum
message. Storage calls have bounded transport waits within a 50-second request
budget; the platform timeout remains 60 seconds. At-least-once delivery can
produce multiple immutable attempt records for the same delivery/input.

The consumer gets create/read permissions conditional on the quarantine prefix,
without overwrite/delete or bucket-list access. See [GCS conditional object
access](https://docs.cloud.google.com/storage/docs/access-control/iam). The existing
400-day lifecycle rules add only the quarantine prefix; versioned deletion also
requires 30 days since becoming noncurrent. Daily archive, historical, operation
and temporary-state policies otherwise retain their previous behavior. No
producer, trigger, Avro, daily DAG or BigQuery schema change is required.

Beam remains the historical backfill engine. Malformed Cocoa/NASA rows and NASA
missing values reach existing invalid BigQuery tables instead of being dropped
by parsing/filtering. Errors retains JSON evidence with source URI/schema,
original source input or sufficient business record, checksum and rejection
time; unknown source acquisition time stays unknown. All three invalid sinks
append, preserving earlier recovery evidence. BigQuery batch load failures fail
the job; original archived input and load artifacts remain available for retry.
Valid historical output publication is still subject to the controlled backfill
and atomic-release work; these scripts are not a safe replacement for the daily
release workflow.

Retired `pipelines/streaming/*_stream.py` entrypoints and stream_start now refuse
startup, matching the approved Cloud Run daily/Beam backfill architecture. Their
historical decoding helpers raise on corruption instead of printing and dropping.
The captured disabled VM Beam installation and prior release artifacts are
preserved; no legacy unit is re-enabled or silently installed from changed source.
Use [retained-input replay](daily-raw-inputs.md) through the supported consumer.

Validation: all 451 Python 3.11 tests, Ruff, 61-file Python/five-script shell syntax
and Terraform validation passed. Native probe `20261029_2c8514b2` ran the exact
consumer ZIP through authenticated Pub/Sub push and real BigQuery/GCS. All twelve
logical cases reconciled: four accepted and eight durably quarantined, with no
overlap. Denied quarantine creation produced 503 before recovery; denied sink
access retained diagnostics and retried before acceptance. Malformed Avro,
valid-Avro/bad-business-data and permanent sink rejection retained exact bodies,
identities and checksums before ACK. Temporary function, subscriptions/topics,
bucket, dataset and function-source prefix were removed. Managed build artifacts
remain subject to the existing artifact repository policy.

The native Python 3.11.18/Beam 2.65.0 image passed local graphs (five valid per
feed; three/two/four rejects). Two cloud executions retained five valid per feed
and accumulated six/four/eight rejects in the existing invalid schemas. Evidence
checksums/source references matched. The disposable dataset/files and temporary
prefix-only VM grant were removed. Initial native checks caught a conditional
IAM policy-version requirement and a NASA header classification error; both
received regression checks before promotion.

The final image also stages backfill_recovery through an adjacent setup.py for
Dataflow, respecting an explicit operator setup file. Its distribution installed
without network access and the installed module bytes matched. Actual Beam setup
options and all three local graphs passed again. Record transforms were unchanged
from the cloud-tested build (AST comparison); remote Dataflow execution was not
performed. The stable VM image tag cocoa-backfill:python3.11 now selects
cocoa-backfill:quarantine-dd98248, image ID
`sha256:56dc432c64b9edaf521ab25d2b7d56047f5c6070c2a751cad949e7a4a55f07ae`.
Prior images and before-quarantine-20261029 remain preserved. No production
historical overwrite was performed.

Production consumer revision stage-analytics-data-00011-kec uses Python 3.11;
its ZIP SHA256 is
`b4ef6b876bf45f4e78481284a35b4440c458a63188bf49456ff0d48fb54db829`.
All five serving members match the tested package. The reviewed plan changed
only that source/environment, two conditional storage grants and the existing
lifecycle prefix list. Producers, source triggers, daily subscription routes,
schemas and DAG are unchanged. Main/bootstrap plans have no drift (114 resources).

Controlled run `quarantine__cad583f__20261029` ran October 29 16:29:13–16:30:09 UTC.
All 74 archived inputs (22 Cocoa, 21 EIA, 31 NASA) match staged revisions, values
and provenance under the new consumer. Cocoa/EIA gates passed; weather failed
because its last valid measurements were October 21, eight days old. SQL/summary
were upstream_failed with no pipeline job. Six serving fingerprints, model etag
and procedure hash stayed unchanged, preserving October 28 outputs. The DAG is
unpaused and idle, legacy daily Beam units disabled, maintenance VM stopped and
scheduled startup retained. No current service-compliant release is claimed.
DLQ inspection/replay/monitoring is item 12; generic atomic publication and
recurring quality remain separate.

Before/accepted consumer source and the completed manifest retain verified holds
under `gs://raw-historic-data/operations/2026-10-29/quarantine-cad583f/`.
Manifest generation `1793292936218019`, SHA256
`38716a038f66be9656b1210144bd1722e43cf2782af4f34c4aa1291f2447ff77`.
Cloud-tested and final worker source bundles are held under quarantine-c74dc74/
and quarantine-dd98248/ respectively. The installed worker distribution is held
at `gs://raw-historic-data/operations/2026-10-29/quarantine-dd98248/cocoa_backfill_recovery-1.0.0.tar.gz`,
generation `1793292935708955`, SHA256
`09b9fb54c69095b7cba77d606b741047b50a76d8295762354136f736de69e60a`.
