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
poison message's budget. The Pub/Sub service agent has prefix-limited create
permission and bucket metadata/list permission; it cannot read message contents
or write the replay ledger. The existing five forwarding IAM pairs, retry
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
Correct the original failure first, then explicitly select one zero-based
record index and its original fully qualified topic:

```powershell
python scripts/operations/replay_dead_letters.py <same-uri> `
  --generation <same-generation> --sha256 <same-sha256> `
  --publish --record 0 --topic 'projects/cocoa-prices-430315/topics/<original-topic>' `
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
and broader on-call routing remain checklist item 25; these policies do not
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

Implementation and native validation receipts will be recorded here after the
isolated poison, storage-outage and controlled-replay checks complete.
