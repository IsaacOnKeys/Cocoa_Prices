# Dead-letter storage and controlled replay

Checklist item 12, branch `codex/dead-letter-recovery`.

The five existing canonical/source-trigger dead-letter subscriptions export
native Avro files, including Pub/Sub metadata, into
`gs://cocoa-prices-430315-dead-letters/messages/v1/<dead-letter-topic>/`.
They retain unacknowledged messages for seven days and finalize files at a
60-second maximum batching duration or approximately 1 MiB. Export failure keeps
messages unacknowledged. Each file includes original binary data, attributes,
export subscription, message ID and publication time. Dead-letter attributes
identify the original subscription, publication time and delivery attempt count.
These are transport failures; application quarantine remains a separate store.

The dedicated bucket has uniform access, enforced public access prevention,
versioning and no soft-delete charge. Message objects become lifecycle eligible
after 400 days; noncurrent versions also need 30 noncurrent days. Replay ledgers
have no automatic expiry, so retaining or holding old evidence cannot reset a
poison message's budget. Replay ledgers live in the separate raw bucket, under
`replay-ledger/v1/`, outside its lifecycle prefixes. Native export requires
bucket-level object-create and legacy bucket-reader grants; object-conditional
grants were rejected during native setup. The service agent cannot read message
contents or write the replay ledger. The existing five forwarding IAM pairs, retry
backoffs and best-effort five delivery attempts remain in place. Five is not an
exact maximum; Pub/Sub forwarding and attempt accounting are best effort.

## Inspect and recover

Use an operator environment with Python 3.11, the pinned operations dependencies,
Application Default Credentials, object read access, create/read/update access
to the replay-ledger prefix, original-subscription read access and publication
permission on the original topic. No new runtime account gets these recovery
permissions. Never place access tokens or API keys in commands or evidence.

List the appropriate message prefix with the Cloud Storage console or CLI and
record an exact object generation. Download that generation and compute SHA256;
retain the reference/checksum in the incident record. Inspection does not send:

```powershell
python scripts/operations/replay_dead_letters.py `
  'gs://cocoa-prices-430315-dead-letters/messages/v1/<dead-letter-topic>/<file>.avro' `
  --generation <generation> --sha256 <sha256>
```

The complete bounded Avro container is verified before any send. The output
contains record count and payload checksums, without printing payloads. Inspect
the pinned file locally with an Avro reader when diagnosis requires its data.
Native export metadata uses a short subscription name in the observed runtime;
the tool accepts that exact name or its fully qualified form, with the original
source project independently verified before replay.
Correct the original failure first, then explicitly select one zero-based
record index and its original fully qualified topic:

```powershell
python scripts/operations/replay_dead_letters.py <same-uri> `
  --generation <same-generation> --sha256 <same-sha256> `
  --publish --record 0 --topic 'projects/cocoa-prices-430315/topics/<original-topic>' `
  --ledger-bucket raw-historic-data `
  --replay-key '<incident-and-remediation-id>'
```

The tool reads the original live subscription and checks that its topic and
dead-letter route still agree with the archive. Deleted, foreign, rerouted and
ordered sources require a separate reviewed recovery procedure. Replay preserves
the exact binary data, acquisition/revision identities and source timestamps.
It removes dead-letter transport attributes and adds recovery identity/attempt
metadata. A source-trigger replay can fetch again and truthfully records that
new acquisition time; it does not impersonate a new Airflow logical run.

Before sending, a generation-conditional ledger reserves an attempt for the
original source, bytes, business attributes and publication time. Concurrent
reservations cannot both publish. Default lifetime budget is one send, including
an uncertain timeout. Reusing the original file or a newly dead-lettered replay
uses the same ledger. There is no automatic replay. After another documented fix,
`--max-replays 2` (hard maximum 3) permits another explicitly named remediation.
After reconciling an uncertain send, `--allow-uncertain` also consumes the next
attempt; it never resets the counter or repeats an existing replay key. Pub/Sub
publication retries are disabled for this operator send. At-least-once transport
still requires downstream deduplication; the ledger is not exactly-once delivery.

## Monitor and diagnose export failures

Terraform manages a dashboard with forwarded message count, regional export
backlog and oldest unacknowledged age. Console incidents cover any successful
forwarding within five minutes and export backlog older than five minutes for
five minutes. Metric sampling/delivery can add delay. Notification recipients
and broader on-call routing remain checklist item 26; these policies do not
send email or chat messages.

For a stalled export, inspect each subscription's `cloudStorageConfig.state`,
bucket existence, service-agent IAM and prefix configuration. Repair the export
without deleting/replacing its subscription. Seven-day retention is the recovery
window for export outages, not a guarantee of unlimited outage survival. Verify
the retained original bytes arrive in a pinned generation after repair. Do not
manually acknowledge unresolved recovery messages.

Sources: [native storage subscriptions](https://docs.cloud.google.com/pubsub/docs/create-cloudstorage-subscription),
[dead-letter forwarding](https://docs.cloud.google.com/pubsub/docs/dead-letter-topics),
[regional backlog and forwarding metrics](https://docs.cloud.google.com/pubsub/docs/monitoring).

## Validation

Implementation commits: `242c034` and `079b2ea`, validated 2026-10-29.
All five production export configurations are ACTIVE. Both Terraform roots
returned live no-change plans, with 120 resources including bootstrap. The
four serving function revisions, six output-table fingerprints, existing model
ETag and deployed SQL definition remained unchanged. Airflow's source hash is
unchanged; it is unpaused and idle, with the VM running in its normal scheduled
window and the three legacy Beam services disabled.

Isolated probe `20261029_28d193d6` used its own topics, pull source, export
subscription and versioned bucket. A poison message exhausted delivery attempts
while storage-create permission was deliberately absent. The native export
reported PERMISSION_DENIED; after restoring access, its retained original bytes
and attributes appeared in Avro. The actual CLI verified/inspected that pinned
file and published attempt one. That poison returned to a second Avro export;
both exports were blocked by the same default budget. An explicit second
remediation replay was delivered and acknowledged, with its original binary
data, acquisition/revision identities, producer timestamp and acquisition key
unchanged. The two confirmed attempts share a single durable recovery root.
The CLI used real cloud clients with the existing gcloud operator credential in
RAM; its ordinary authentication path expects ADC.

The five scoped forwarding pairs and source retries were already tested during
[item 8](retry-delivery.md), including an actual Gen2 trigger. This new probe
tests native storage export and operator replay, without redeploying a function
or running a production backfill. The isolated bucket, both subscriptions,
both topics and all versions of its operator replay ledger were deleted and
absence verified. Exact native Avro evidence and the recovery source archive
are checksummed, generation-pinned and held under
`gs://raw-historic-data/operations/2026-10-29/dead-letter-242c034/`.

The completion manifest is `completed_manifest.json` in that prefix; its exact
generation/SHA256 are recorded in the ignored parent checklist and private
`.build/dead-letter-recovery/state.json`. All 466 credential-free tests passed;
Ruff, 65 Python syntax checks, five Bash syntax checks and Terraform format and
validation checks passed. Tests cover uncertain publication, repeat keys,
concurrent reservation conflicts, route/provenance mismatch, and the hard
three-attempt ceiling. Retention policies are configured, not tested by waiting
400 days. The monitoring API accepted the policies and dashboard; notification
delivery, rendered dashboard acceptance and overall service reliability are
not claimed. NASA's previously verified eight-day source age still withholds
the current release, preserving October 28 serving outputs/model.
