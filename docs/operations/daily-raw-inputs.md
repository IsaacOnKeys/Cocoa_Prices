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
The request descriptors belong to archive format v1. Source-interface changes
must retain the v1 reader contract or introduce a version with backward reading
support; changing a descriptor must not invalidate retained inputs.

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
syntax and Terraform validation passed. Managed probe `20261029_72bb4cf0`
verified twelve complete archives, including three partial sends that confirmed
only their first row. After original deliveries were acknowledged, retained live
inputs reconstructed 25 Cocoa, 24 EIA and 36 NASA revisions through real Pub/Sub
and the unchanged actual consumer into disposable BigQuery tables. Source API
calls were absent from reconstruction. Values, original timestamps, raw payloads,
acquisition identities and business revisions matched. Consumer execution for
this isolated sink check was local Python 3.11, using actual authenticated
insertAll writes; it was not a new Cloud Run consumer deployment. The native
bucket configuration and recovery of original bytes after overwrite/deletion
passed. Four hundred days of aging were not elapsed-time tested. Every temporary
function, transport, bucket, source archive and dataset was removed.

The reviewed production plan updated only the three producer sources and raw
bucket versioning/lifecycle. No IAM, trigger, consumer, Avro, Beam, SQL or DAG
configuration changed. All three deployed ZIPs/members match the tested sources:

| Feed | Serving revision | ZIP SHA256 |
| --- | --- | --- |
| Cocoa | publish-price-00022-kaz | 1ac9b22d0ad1c0ceb34c39a8a5eb6e6d24f7b3302db9d918d60dffa70d5ac19f |
| EIA | publish-eia-data-00007-com | 2a6c8193f4bca7eb9c510172b10cb556399de811eb73f0a0251deeb7c21d86cb |
| NASA | publish-weather-data-00016-nob | 2b3d59cdda91279d34a37ea5c6517cc54309560623e5712e1e7a96924e239135 |

Controlled run `daily_raw__e30e8e6__20261029`, October 29 15:15:08–15:15:43 UTC,
archived and staged 22 Cocoa, 21 EIA and 31 NASA revisions. Each exact input and
outcome generation/checksum, receipt inventory, wire values, raw payload and
staging provenance was reconciled. The CLI verified/exported all three production
batches without publishing recovery messages, using existing operator credentials
in memory. Cocoa and EIA passed their gates. NASA's last valid precipitation and
soil moisture remained October 21: weather failed at age eight, above seven.
SQL/summary were upstream_failed with no pipeline job. Six serving fingerprints,
model etag and procedure hash stayed unchanged; October 28 outputs remain visible.
The Python 3.11.10 Airflow DAG is unpaused/idle; disabled daily Beam units and
normal scheduled startup are retained. The maintenance VM is stopped, and both
Terraform roots have no drift (112 managed resources). No current service release
is claimed. Other release, quarantine and quality work remains separate.

Verified source/rollback archives and the completed manifest retain temporary
holds under `gs://raw-historic-data/operations/2026-10-29/daily-raw-e30e8e6/`.
Manifest generation `1793287278417478`, SHA256
`e1e34b68abf1a089b027324b7b7e0a5811fd082dccae2b86a3851f6503b90aaf`.
