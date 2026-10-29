# Replayable daily inputs

Checklist item 10, branch `codex/daily-raw-archive`. When the production coverage
buckets are configured, all three producers must persist the complete serialized
batch before initializing a publisher. A storage failure withholds publication.
This uses the existing raw bucket and producer create/read permissions; no new
service, IAM grant, schema change or consumer change is required. Unconfigured
local/legacy invocations retain their previous behavior.

`gs://raw-historic-data/daily-inputs/v1/<source>/<UTC-date>/<acquisition-id>/batches/<SHA256>.json`
contains every exact Avro message and its SHA256, transport attributes, the
embedded schema/version/checksum, original acquisition/fetch completion time,
requested window, source identity, units and sanitized request descriptors.
These are sufficient wire records with source values/provenance, rather than a
claim to archive the original ICCO HTML or EIA HTTP response. NASA's existing
raw_payload retains its complete response. EIA request metadata is an explicit
allowlist, independent of API keys, request URLs containing credentials or SDK
exceptions. No credentials are written to an archive or replay log.

The immutable prepared batch survives partial publication or process termination.
Separate content-addressed outcomes record confirmed ACK counts and either
publish_acknowledged or publication_unconfirmed; an absent outcome means unknown.
Failure to persist a successful outcome fails acquisition and cannot advance
coverage. Successful coverage receipts link both exact object generations and
checksums. A replay may recover all rows even if only the first was originally
acknowledged. Neither prepared inputs nor ACK outcomes imply sink acceptance or
fresh-source eligibility. Failures before a valid serializable batch exists
remain failed acquisitions; invalid-record quarantine is item 11.

Raw bucket versioning and two prefix-scoped lifecycle rules provide a 400-day
daily-input archive, followed by at least 30 days as a noncurrent version. An
earlier accidental delete/overwrite also retains the noncurrent input until both
age conditions are met. Producers use create-only writes and cannot replace an
existing input. Historical prefixes, source-code archives and held operation
evidence are outside the new lifecycle rules. The temporary state bucket's
policy is unchanged. GCS lifecycle processing is asynchronous; eligibility is
not an exact deletion time. See [Google's lifecycle conditions](https://docs.cloud.google.com/storage/docs/lifecycle).

To verify and export an interval, use Python 3.11 with requirements-ops and
Application Default Credentials for an authorized operator:

```powershell
python scripts/operations/replay_daily_inputs.py gs://raw-historic-data/daily-inputs/v1/SOURCE/DATE/ACQUISITION/batches/SHA256.json --generation GENERATION --output .build/recovered-inputs.jsonl
```

Use the exact URI/generation from raw_input in the acquisition receipt. Export
creates a new file and never overwrites one. The complete batch, embedded schema,
message checksums, provenance and business identities are validated before any
replay send. The command accesses only GCS and optional Pub/Sub; it never queries
a source API. Publishing additionally requires `--publish --topic
projects/PROJECT/topics/DESTINATION --replay-key INCIDENT-ID`. The recovery
destination is explicit; only the replay-key hash is added as replay_id.
Original bytes, acquisition/observation IDs and source timestamps are preserved.
Recovery deliveries therefore cannot impersonate the current Airflow attempt.
Transport retries remain at-least-once and can duplicate physical staging rows.
Replay failure may leave a partial batch; rerun the same retained input using
the recovery identifier and reconcile business revisions, not physical counts.

Validation: 401 Python 3.11 unit tests, Ruff, 55-file Python/five-script shell
syntax and Terraform validation passed. Managed probe, deployed source and
retention verification remain pending. No current service release is claimed:
the NASA freshness breach continues to be governed by the existing mandatory
source gate. Other release, quarantine and quality work remains separate.
