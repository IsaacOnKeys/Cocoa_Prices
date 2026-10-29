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

Validation currently includes 438 full Python 3.11 tests plus focused namespace
checks for the new managed probe, Ruff, syntax and Terraform validation. Native
Cloud Run and rebuilt Beam-container validation and production promotion remain
pending. DLQ inspection/replay/monitoring is item 12; generic atomic publication
and recurring quality are separate items. The NASA freshness breach continues
to withhold current releases.
