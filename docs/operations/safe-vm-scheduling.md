# VM scheduling and bounded recovery

Checklist item 27, branch `codex/safe-vm-scheduling`. Keep the existing small
VM and the fixed shutdown rather than adding a continuously running controller.

## Schedule and readiness

Automatic startup is **18:30 Monday-Friday Europe/Berlin**. Shutdown remains
**20:30 every day**, including weekends, to bound accidentally retained manual
maintenance sessions. Both existing Scheduler jobs have two retries, a
30-second request deadline and 30-60 second backoff (about three minutes of
configured attempts/backoff). Retry duration is zero to use the count alone;
setting both limits can produce extra retries under
[Scheduler's retry semantics](https://docs.cloud.google.com/scheduler/docs/configuring/retry-jobs). API acceptance
does not prove that Airflow is healthy; the independent watchdog remains the
missed-start/deadline detector. There are no new Scheduler jobs or VM resources.

The enabled `airflow-compose.service` runs the Python 3.11
`scripts/operations/airflow_runtime.py`, installed at
`/usr/local/lib/cocoa/airflow_runtime.py`. Startup fetches the existing Fernet
secret without printing it, starts only installed images (no pull/build), waits
up to four minutes for Compose health, then verifies the Airflow Google
connection's credential refresh and required producer/procedure configuration.
PostgreSQL/scheduler/web health checks remain those of the installed Compose
configuration. Producer endpoints and required source receipts retain their
existing per-task checks; startup does not acquire data or submit pipeline SQL.

The service has a 330-second start timeout, a 30-second restart delay and at
most two starts per 30-minute interval. A failed start emits `start_failed` to
the existing critical alert route; a successful start records `startup_ready`.
If central logging fails, diagnostics remain in `journalctl -u
airflow-compose.service`. Fix the underlying problem before `systemctl
reset-failed airflow-compose.service` and another intentional start.

## Admission, stale work and catch-up

The DAG retains one active run, `catchup=False` and its 19:00 weekday schedule.
Scheduled runs now have a **60-minute DAG timeout**, with a two-minute default
task timeout; existing shorter/longer task-specific bounds remain explicit.

All production execution uses the run's business date and the current Berlin
clock, including after restart:

| Stage | Allowed local execution start |
| --- | --- |
| Capture/admit a run | 18:30 through 19:29 |
| Trigger a producer | 18:30 through 19:44 |
| Enter publication or submit a new warehouse attempt | 18:30 through 20:04 |

A previous-day run cannot resume acquisition or publication today. The
publication gate is checked again before each new internal warehouse attempt,
so a long failed attempt cannot start its replacement after the cutoff.
The existing warehouse request timeout is 20 minutes; service-side cancellation
is not a guaranteed spending cap. Existing deterministic job IDs, atomic
publication and accepted forecast/model checks remain the recovery mechanism.
Explicit `engineering_validation=true` runs retain their existing window
exemption and do not establish service delivery or extend the VM schedule.

`catchup=False` is not historical repair. A delayed same-day scheduler may
admit the current scheduled interval before 19:30. After that, preserve the
last accepted release, investigate the alert and recover by the next operating
day's deadline. Historical omissions belong to the separately governed
backfill/repair path, not repeated daily producer invocations. A same-day manual
recovery before the admission cutoff must be a new run; do not blindly clear
old tasks or change their acquisition identity.

## Fixed shutdown and interrupted work

Shutdown deliberately **does not extend VM uptime for active work**. Before
stopping Compose, the helper inspects active/queued Cocoa runs with a bounded
metadata read. It emits `shutdown_interrupted` with run IDs when work remains,
or `shutdown_state_unknown` when inspection fails. Both use the existing ERROR
email route. Containers receive up to 20 seconds to stop; the service has a
75-second stop limit. Stopping uses no Secret Manager read. PostgreSQL's
existing persistent volume, task logs and recovery copies are retained.

If a run is interrupted:

1. Read the alert/run metadata and retained task logs. Identify the deterministic
   `cocoa_release_<release_id>_<attempt>` job from the release request/XCom.
2. Inspect that exact BigQuery job in `europe-west3` and the release registry.
   VM shutdown does not itself cancel warehouse work; uncertain completion must
   be reconciled before another attempt. A completed atomic release may already
   be accepted even if its Airflow task did not record success.
3. Keep accepted outputs visible. Start a new eligible recovery run only after
   reconciling the original; use its new acquisition identity and normal gates.
   Never delete raw receipts, truncate serving tables, retrain or extend uptime
   just to make the scheduler appear successful.

This is a documented recoverable-interruption policy, not uninterrupted service
through an arbitrary VM cutoff. Backup/restore of lost runtime state remains
item 28; safe interval repair remains item 29.

## Validation

Eleven focused Python 3.11 tests cover delayed admission, stale-day restart,
winter/summer clock boundaries, publication retry cutoff, unavailable secrets,
bounded unhealthy dependencies, credential readiness, active-run shutdown and
metadata failure at shutdown. Changed-file Ruff and Terraform validation passed.
Only the two existing VM Scheduler jobs were planned/applied and their live
weekday/timezone/retry settings verified. No producer, model-training or full
pipeline validation was requested for this feature.

On 2026-10-30 the installed service restarted successfully (`ActiveState=active`,
`SubState=exited`, `NRestarts=0`) and recorded `startup_ready`. The native Airflow
image imported the changed DAG and verified its callable wiring and 60-minute
run limit. A simulated clock accepted delayed admission and rejected stale-day
restart and late publication. The selected historical successful run and all
nine task states were unchanged across the service restart. Active-work cutoff
and unavailable-metadata shutdown paths were exercised in the focused tests;
no live production batch was interrupted as a drill.

Installed DAG SHA256:
`9f129d1061d3b4f558013609dad50fc82883b193e321e530604d7a8cefd28112`.
Prior files remain in
`/opt/airflow/releases/safe-vm-scheduling-20261030-native2`. Setup initially
raced the existing initialization job, then a simulated-clock fixture error
caused rollback; both checks were corrected before the successful installation.
Local receipts are under `.build/safe-vm-scheduling/`. The maintenance VM was
returned to its original stopped state after validation.
