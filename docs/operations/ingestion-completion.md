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
direct required upstreams of the SQL operator. Installation and a controlled live
run remain pending.

This completes the ingestion-boundary mechanism, not a frozen input snapshot.
The procedure still reads live staging; deterministic revision precedence,
recurring quality and reproducible/atomic candidate publication remain items
15, 17 and 19-20. The current NASA age breach must continue to block SQL, keeping
the October 28 serving outputs visible. No fresh service release is claimed.
