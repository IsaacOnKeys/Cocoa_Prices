# Production execution decision: checklist item 3

Status: proposed, 2026-10-27, branch codex/production-execution. This decision
changes where Apache Beam participates in the project. Confirm whether Beam
must remain in daily production before implementing either alternative.
The verified EIA production route remains deployed unchanged.

## Workload and current limits

The contract requires one complete weekday release by 20:00 Europe/Berlin,
not continuous subsecond analytics. Cocoa acquires one latest quote per
invocation; oil acquires a fourteen-day interval; weather requests eight
inclusive calendar dates. The controlled EIA cycle delivered six oil records
and completed all ten Airflow tasks in 76 seconds. This is a small daily
workload, not a load test or a source-availability guarantee.

All three streaming scripts hardcode DirectRunner. They run alongside Airflow
and PostgreSQL on one scheduled e2-medium VM. The cutover verified them healthy,
but that does not establish a supported production runner or remove their
shared failure domain. [Apache Beam](https://beam.apache.org/documentation/runners/direct/)
describes DirectRunner as a development/test runner and reports limited Python
streaming support. Moving the same runner into another container would not
resolve this finding.

The current Python Beam pin is 2.65.0. Google's
[Dataflow support matrix](https://docs.cloud.google.com/dataflow/docs/support/sdk-version-support-status),
checked 2026-10-27, lists it as deprecated since May 12, 2026. A Dataflow
implementation must choose/test a currently supported SDK; changing only the
runner flag is insufficient. Existing backfill dependency/overwrite defects
remain separate work and must not be exercised against curated production.

## Alternatives

| Approach | Daily processing | Operational consequences | Project emphasis |
| --- | --- | --- | --- |
| Recommended: bounded authenticated Cloud Run consumers | Keep source functions, triggers, schema-bound Pub/Sub topics and Avro contracts. Replace VM streaming consumers with finite HTTP handlers invoked through authenticated Pub/Sub push. | Consumers run independently of the Airflow VM; return success only after durable acceptance. Bound time, payload, memory and instances. Preserve retryable failures. | Beam remains for historical/backfill work and local compatibility testing. |
| Supported production Beam on Dataflow | Keep Beam in the daily path; make runner/configuration explicit and deploy tested managed jobs. | Upgrade SDK, stage schemas/dependencies correctly, define readiness and drain/restart behavior, isolate service accounts and control worker counts. Scheduled streaming still requires bounded lifecycle management. | Demonstrates managed production Beam, with additional infrastructure and worker operation. |
| Bounded Cloud Run jobs reading acquisition artifacts | Execute finite staging jobs against explicit source manifests. | Strong fit once durable acquisitions/completion receipts exist, but introducing it immediately also changes acquisition handoff and orchestration. | Consider with raw-retention/run-manifest work if whole-acquisition jobs become preferable. |

Google documents [authenticated Pub/Sub delivery to Cloud Run](https://docs.cloud.google.com/run/docs/tutorials/pubsub)
and its [success/failure acknowledgement protocol](https://docs.cloud.google.com/pubsub/docs/push).
Pub/Sub retry and retention do not establish exactly-once warehouse effects.
Durable quarantine, delivery identity, idempotency and DLQ replay remain explicit
acceptance work; replacing the runner does not automatically complete them.

## Cost and recovery comparison

Cloud Run consumers can scale to zero between requests. Their relevant charges
include execution, builds/artifacts, Pub/Sub, logs and warehouse writes.
[Cloud Run pricing](https://cloud.google.com/run/pricing) depends on region and
execution mode; free allowances are shared by billing account, so zero cost is
not promised. Initial proposed limits are no minimum instances, maximum three
instances, one request per instance, 512 MiB memory and a 60-second timeout;
the isolated test must establish adequate headroom before accepting them.

[Dataflow pricing](https://cloud.google.com/dataflow/pricing) includes worker
CPU/memory and other job resources, even for a low-volume stream. Three jobs
kept up for the current two-hour VM window imply six worker-hours per operating
day at one worker each, before startup/drain extensions and worker sizing.
Consolidation could reduce that baseline but adds deployment coupling. Neither
alternative has a measured regional bill yet. Obtain an europe-west3 estimate
from the chosen configuration and measure a capped isolated run before cutover;
do not quote the pricing page's default Iowa rates as Frankfurt prices.

Moving consumers off the VM reduces the ingestion failure domain. Airflow's
VM/PostgreSQL remain an orchestration dependency under both choices. Meeting
the accepted next-operating-day recovery objective still requires the external
missed-run alerts, runtime backups and restore exercise in items 26-28. A
multi-node orchestrator is not justified solely by a once-daily workload.

## Concrete implementation and acceptance gates

For the recommended approach:

1. Implement one bounded consumer using the three canonical Avro schemas and
   explicit subscription-to-feed/table routing. Preserve source units and
   provenance; retain existing wire compatibility. Package without Beam.
2. Verify real binary payloads for all feeds, nullable fields, malformed input,
   unknown routes, transient/rejected warehouse writes, timeouts and redelivery.
   No successful HTTP response may precede accepted storage. Do not silently
   drop invalid inputs while quarantine/DLQ work remains open.
3. Define private invocation, a dedicated push identity and least necessary
   staging access. Deploy only to isolated topics/subscriptions/tables first,
   with finite resource limits and no production consumer competition.
4. Exercise real Pub/Sub authentication, schema decoding and BigQuery writes;
   measure cold startup, duration/memory, failure/retry and recovery. Verify
   new consumers continue when the Airflow VM is stopped. Record actual cost
   dimensions and readiness evidence.
5. Review the exact production subscription/routing changes. Stop old consumers
   before handing off their subscriptions; preserve backlog and rollback
   configuration. Verify every feed and the full SQL/BI cycle. Remove the
   daily DirectRunner startup dependency only after acceptance.
6. Update installed units/runtime ownership, Terraform, diagrams, deployment
   docs and inventory to match the chosen serving implementation. Keep local
   Beam examples explicitly scoped and do not claim item 3 complete until the
   deployed startup/restart/recovery checks pass.

For Dataflow, use the same isolated data/acceptance checks with a supported SDK,
runner options, staged schemas, service-account permissions and explicit
managed-job startup/drain/restart/rollback procedures. No Dataflow job, Cloud Run
consumer or production subscription change was deployed during this comparison.
