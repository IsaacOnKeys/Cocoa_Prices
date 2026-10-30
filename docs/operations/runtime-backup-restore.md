# Runtime backup and restore

Checklist item 28, branch `codex/runtime-backup-restore`. Back up the Airflow
runtime outside its VM; warehouse releases, raw evidence and managed producer/
consumer services retain their separate cloud recovery paths.

## Scope, schedule and cost controls

`cocoa-airflow-backup.timer` runs at **20:10 weekdays Europe/Berlin**, before
the 20:30 VM cutoff. Its persistent setting catches missed timer executions
when the VM next starts. No additional VM, database service or Scheduler job is
created. The oneshot service allows 15 minutes, uses a process lock and runs
at reduced CPU priority. It has no automatic retry loop. Shutdown stops it
before Airflow. An interrupted/failed script emits `backup_failed` through the
existing critical email route; its last `backup_stage` is in the service journal.

The dedicated private regional bucket is
`gs://cocoa-prices-430315-runtime-backups`. Public access is enforced off;
the VM receives object creation/read access without deletion or overwriting.
Uploads use generation-zero preconditions. State archives expire under a
14-day lifecycle policy; deletion is asynchronous. Each compressed state
archive is limited to 64 MiB. Normal weekday delivery creates one archive and
one small receipt per operating day; intentional manual runs add more.

Airflow and PostgreSQL image archives are saved once per immutable image ID,
then reused. Each compressed image is limited to 2 GiB, with at most four image
objects. A fifth image blocks backup until the owner reviews retained archive
references and removes genuinely unused images. Images do not silently expire
while current backups still depend on them. This avoids exporting large images
every day or maintaining a registry service. Image uploads disable temporary
parallel-composite objects; they do not require deletion privileges.

This bounds the configured workload, not the total cloud bill. Existing budget
notifications and the under EUR 5/month target / EUR 10 maximum remain in force;
actual billing spend and the broader cost baseline remain item 34.

## What is preserved

The encrypted state archive contains:

- A consistent `pg_dump` custom-format copy of the actual `airflow` PostgreSQL
  database, including users, Variables, connections, DAG runs, task states and XCom.
- Installed Compose files/overrides, `.env`, configuration, DAGs and plugins.
- The Airflow systemd unit, backup service/timer, runtime/backup/restore helpers
  and any recovery-key drop-ins.
- A manifest of file SHA256 values, image IDs and exact image-object generations,
  the dump timestamp, and a retained successful run/task-state reference.

Task-log files and old local release directories are excluded. Delivery
manifests and central logs remain outside the VM; image archives and the
installed source/configuration recover the daily runtime without rebuilding
packages from a mutable dependency solver. Disabled legacy Beam services are
not part of the daily runtime; backfill source/build definitions remain in Git.

The existing Terraform backend already stores both `terraform/main` and
`terraform/bootstrap` state outside the VM. Its bucket has versioning and
30-day soft deletion. Validation downloads/parses both exact live generations;
it does not push an old state file or apply an historic infrastructure plan.
Keep the source checkout and backend identifiers available to the operator.

## Secrets and integrity

The backup worker uses the installed Python 3.11 Airflow image's existing SDK
and cryptography dependencies. The state archive is authenticated/encrypted
with the active Fernet key before upload. It verifies that the running key
matches the selected Secret Manager version. Receipts record the **resolved
numeric version**, ciphertext hash/size and immutable object generation.
Keys are never committed, printed or saved beside the ciphertext. Images have
Google-managed storage encryption and are private; their content hashes are
inside the authenticated state manifest.

Keep every referenced `airflow-fernet-key` version enabled for the lifetime of
its retained backups. Secret Manager and its IAM grants are outside the VM;
Terraform protects the secret container from destruction. This recovery scope
assumes the project/account and referenced secret versions still exist. It
does not provide recovery after deletion of the whole project or all keys.

`COCOA_FERNET_VERSION` can pin a recovered runtime and its backup worker to a
numeric version; normal operation continues to use `latest`. Extraction writes
`recovery-key.conf`, containing only that version reference. Install it as a
drop-in for **both** `airflow-compose.service` and
`cocoa-airflow-backup.service` before starting a recovered runtime. Review key
rotation separately; do not disable old versions merely because a new one is
latest. [Airflow's Fernet guidance](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/fernet.html)
explains the ordered-key rotation procedure.

## Backup and isolated verification

Run as root on the existing VM:

```bash
systemctl start cocoa-airflow-backup.service
journalctl -u cocoa-airflow-backup.service -n 30 --no-pager -o cat
```

Use the `receipt_object` and `receipt_generation` from `backup_completed`:

```bash
python3 /usr/local/lib/cocoa/airflow_restore.py \
  --receipt-object runtime/EXACT_RECEIPT.json --receipt-generation GENERATION
```

The restore tool needs Python 3.11, Docker and gcloud, plus the Airflow service
account's backup-read/secret-access permissions. It does **not** require the
original Airflow container or its volume. A digest-pinned public Airflow base
image supplies Fernet for bootstrap; the authenticated manifest then identifies
the saved production images. [Generation-specific objects and preconditions](https://docs.cloud.google.com/storage/docs/request-preconditions)
prevent substitution by a later object version.

The check downloads/decrypts the saved archive, rejects unsafe paths, links,
duplicates and mismatched file hashes, and verifies/loads the saved images.
It restores PostgreSQL into a uniquely named disposable volume/network. In
that isolated database it pauses all DAGs and marks unfinished work failed
**before** starting a scheduler. It checks retained task states, decrypts all
Variables/connections, refreshes the Google credential, imports the saved DAG
and waits for scheduler health. No producer, warehouse release or model job is
launched. The replica uses a separate database, no published ports and bounded
container memory; containers, volume and network are removed after the check.

## Recovery onto a replacement runtime

Preserve any surviving production disk/volume before replacement. Use a
reviewed infrastructure recovery plan for the existing Debian 12 topology,
service account and scopes; this tool never replaces a live VM or Terraform
state. Install [Docker/Compose](https://docs.docker.com/engine/install/debian/),
[gcloud](https://docs.cloud.google.com/sdk/docs/install-sdk) and Python 3.11,
then copy the two recovery Python files from the checkout into one directory.

1. Run the isolated check above to verify the chosen generation and load its
   saved runtime images. Reconcile deterministic warehouse jobs and accepted
   releases using the [interruption runbook](safe-vm-scheduling.md).
2. Run the same restore command with `--extract-to /root/cocoa-recovery-NEW`.
   The destination must not exist. It materializes verified files and the
   non-secret key-version reference; it does not overwrite a live runtime.
3. Install the saved Compose/configuration/DAG/plugin files at their original
   `/opt/airflow` paths, helpers at `/usr/local/lib/cocoa`, and saved units under
   `/etc/systemd/system`. Keep `.env` root-owned mode 0600. Restore the Compose
   runtime UID/GID for DAG/config/plugin/log directories (currently 1001:0);
   use directories 0750 and readable files 0640 so the container can read them.
   Materialized staging files are deliberately root-private, not installation
   permissions. Install `recovery-key.conf` under both unit `.d` directories.
4. Start **PostgreSQL only** from the saved Compose configuration, into a new
   volume. A placeholder Fernet interpolation is sufficient for this step;
   do not start scheduler/web/triggerer yet. `pg_restore -U airflow -d airflow
   --no-owner --no-acl` restores `postgres.dump`. Keep the original volume.
5. Apply the isolated check's safe mode to the recovered database: pause DAGs,
   mark unfinished runs/tasks failed, and reconcile their exact cloud jobs.
   Verify decryption using the pinned secret version. Do not copy failed
   engineering runs into a claim of successful service delivery.
6. Reload/enable the installed systemd units, then start Airflow and verify
   health/configuration. Enable the backup timer. Unpause `cocoa_daily` only
   after the recovery window and cloud-job reconciliation are understood;
   perform any acquisition recovery under the normal admission/quality gates.

The agreed RPO is at most one operating day of acquisition loss; the RTO is
restoration by the next operating day's deadline. A nightly dump cannot repair
an upstream publisher's lost historical observations. The drill below measures
an isolated runtime restoration on the existing VM, avoiding another paid VM;
it is not a fresh-OS installation or a whole-project disaster exercise.

## Validation evidence

Fifteen focused Python 3.11 tests and changed-file Ruff passed. Checks cover
archive paths/links/duplicates, corrupted bytes, immutable key references,
no-overwrite extraction, separate download staging and pinned-key startup. Terraform format/validation
passed. Native infrastructure/state-read and backup/restore receipts are under
`.build/runtime-backup-restore/`.

On 2026-10-30 the native restore completed in **116.39 seconds**, including
download/decryption, saved-image verification/loading, PostgreSQL restoration,
retained task-state checks, all Variable/connection decryption, credential
refresh, saved DAG import and paused scheduler health. The disposable
`cocoa-restore-a0b56d2aedd8` containers, volume and network were removed and
absence verified. Production remained idle and DAG SHA256 remained
`9f129d1061d3b4f558013609dad50fc82883b193e321e530604d7a8cefd28112`.

Verified receipt:
`runtime/20261030T224454Z_5375d2596cbd4fd799cda11fedea7d47.json`, generation
`1793400295400606`. Its database snapshot began at **22:44:49 UTC**; the
1,656,076-byte encrypted state object was accepted at **22:44:55 UTC**, generation
`1793400295098477`, SHA256
`43fe1e73f21e4c4a31894cf6c7028dd30350b2375b199bef342ecbe123f35675`.
The resolved key reference is Secret Manager version 1, not a mutable alias.
Two reusable image archives total **625,530,674 bytes** (about 597 MiB).
These are measured storage sizes, not a measured monthly bill.

The enabled timer's next tick was verified as **2026-11-02 20:10 CET**.
The first drill exposed an extraction-staging bug; separating downloaded and
verified files and using Airflow's normal container entrypoint corrected it
before the successful drill. The maintenance VM was returned to its original
stopped state. Installed recovery copies/results remain at
`/opt/airflow/releases/runtime-backup-restore-20261030`.
