# Receipt-based ingestion completion

Checklist item 14, branch `codex/ingestion-completion`. The source helper contract
from [item 13](required-feed-completion.md) is unchanged. This change removes the
obsolete sleep_after_ingest TimeDeltaSensor and its PROGRESS_QUIET_SEC read.
BigQuery now directly requires ALL_SUCCESS from all three receipt/inventory gates.
There are nine tasks. Stored old quiet-period Variables are preserved for history
and rollback but no longer affect the DAG.

TimeDeltaSensor measured from data_interval_end; it never established a delay
from completion of ingestion. For late/manual runs that time could already have
passed. Publication now depends on exact intended acquisition/attempt receipts,
source eligibility and every declared staged revision being queryable. There is
no arbitrary replacement sleep, increasing-count shortcut or one-feed fallback.

The existing native isolated GCS/BigQuery proof remains applicable: a declared
31-row batch with thirty staged revisions waited; adding only its final row made
all required gates pass. The gate's helpers are unchanged. An additional Python
3.11 regression uses a manual run whose interval end is twenty-eight days old:
it still waits for the missing row and passes only after it arrives. Native DAG
validation verifies no time-based sensor, nine tasks and all three gate tasks as
direct required upstreams of the SQL operator. All 385 Python 3.11 tests, Ruff and
syntax checks (53 Python files, five shell scripts) passed. Native validation used
Python 3.11.10, Airflow 2.10.2 and Google provider 10.22.0.

Installed DAG SHA256:
`491c69f0bf96c91ea13565193a65e4ece55c3f8cee09fd7bf8ed10acc7e14cf1`;
UID 1001, GID 0, mode 0644. Scheduler serialization confirmed nine tasks and the
three direct prerequisites. The root-only before-copy remains at
`/opt/airflow/releases/ingestion-completion-20261029/cocoa_daily.before.py`.

Controlled run `ingestion_complete__33c1651__20261029` ran on October 29 from
11:30:10 to 11:30:58 UTC. Collection succeeded for all feeds; Cocoa and EIA passed
their gates. Weather failed because precipitation was eight days old, exceeding
seven. SQL and summary were upstream_failed; no pipeline job was submitted. All
six serving-table fingerprints, the model etag and procedure hash stayed
unchanged. The DAG is unpaused and idle; the maintenance VM is stopped, with
normal scheduled startup retained. Legacy daily Beam units remain disabled.
Both Terraform roots returned no-change plans (112 managed resources).

Before/accepted DAG bytes and the completed manifest are retained with verified
temporary holds under
`gs://raw-historic-data/operations/2026-10-29/ingestion-completion-33c1651/`.
Manifest generation `1793274213111141`, SHA256
`4503776e4d99a8b3fa16903c8de7a39d1a0da7f4c13b5ff118f388f5564c4a7f`.

This completes the ingestion-boundary mechanism, not a frozen input snapshot.
The procedure still reads live staging; deterministic revision precedence,
recurring quality and reproducible/atomic candidate publication remain items
15, 17 and 19-20. The current NASA age breach must continue to block SQL, keeping
the October 28 serving outputs visible. No fresh service release is claimed.
