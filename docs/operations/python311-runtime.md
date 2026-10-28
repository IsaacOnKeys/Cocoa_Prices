# Python 3.11 runtime standardization

Owner requirement added 2026-10-28 under checklist item 31. Branch:
codex/python311-runtime. Status: source/build definitions prepared; container,
isolated runtime IO and production cutover gates pending. This is a bounded
runtime migration; the remaining acceptance criteria of item 31 stay open.

## Scope and baseline

Use Python 3.11 for development, CI, Airflow, Beam backfill clients/workers and
all active functions. Existing code already passes 198 credential-free tests,
Ruff and syntax under Python 3.11.9. Development dependencies pass pip check.
The nine existing Beam pins resolve to 111 packages on that interpreter;
resolution alone does not establish Linux native/runner compatibility.

The actual Airflow runtime is 2.10.2 with Google provider 10.22.0 on Python
3.12.6. Keep the Airflow/provider versions, task graph, schedule, PostgreSQL,
Variables, connection/Fernet configuration and mounts unchanged. The VM's
native Compose default is apache/airflow:3.0.4, overridden by
AIRFLOW_IMAGE_NAME=apache/airflow:2.10.2 in .env. Both image selectors must move
to the validated 3.11 build; copying the sanitized repository capture alone
would not establish that change. Exact original Compose/.env copies are private
mode 0600 in /opt/airflow/releases/python311-20261028/. The prior image is also
retained as cocoa-airflow:rollback-python312-20261028.

The three producers already use python311. Only the managed daily consumer's
function runtime needs changing from python312. Preserve its tested timestamp
source package, routes, IAM and limits. The runtime-only plan resubmitted the
older configured seed source; the post-deployment byte/AST check caught the
regression before acceptance while the DAG was paused. The consumer now pins
the verified timestamp package in Terraform and removes source ignore_changes.
Update that immutable source pointer with future consumer code releases. Verify
actual serving bytes and all four runtime IDs after promotion; no-drift alone
is not source-equality evidence.

## Build definitions

- [Airflow Dockerfile](../../airflow/Dockerfile): explicit
  apache/airflow:2.10.2-python3.11 base with its verified digest, existing pinned
  Airflow/Google-provider/Pendulum requirements and pip check. Run pip as the
  inherited airflow user. Build from the repository root; .dockerignore permits
  only its runtime inputs. Retain the base entry point and Compose user/mounts.
- [Backfill Dockerfile](../../pipelines/backfill/Dockerfile): explicit Python
  3.11-slim base digest, the complete existing pipelines/requirements.txt pins
  and pip check. Build from pipelines/, retaining /app scripts and Python entry
  point. Align Beam to 2.65.0 as already specified by the dependency manifest.
- CI selects only 3.11; .python-version and Ruff target py311. Keep development,
  Airflow and Beam environments separate instead of merging incompatible SDK
  families into one installation. Windows developers must select py -3.11 or
  their 3.11 virtual-environment interpreter rather than ambient Python.
- Historical receipts keep their recorded versions. Record actual image IDs,
  base digests, source revision, installed dependencies and test results for
  this migration rather than rewriting earlier Python 3.12 evidence.

Airflow documents explicit
[Python image variants and same-version extension](https://airflow.apache.org/docs/docker-stack/build.html).
Beam's [compatibility table](https://beam.apache.org/documentation/sdks/python/)
includes Python 3.11 for Beam 2.47.0 onward. Google currently schedules python311
[deprecation/decommission](https://docs.cloud.google.com/functions/docs/runtime-support)
for 2027-10-24/2028-04-24. Record a future runtime review before that deadline;
this standardization does not make 3.11 an indefinite support promise.

Build both images explicitly before starting the captured Compose services:

~~~bash
docker build -f airflow/Dockerfile -t cocoa-airflow:2.10.2-python3.11 .
docker build -f pipelines/backfill/Dockerfile -t cocoa-backfill:python3.11 pipelines
~~~

The cloud-control/diagnostic SDK set is pinned in requirements-ops.txt and runs
in a separate Python 3.11 environment. Both thin development and operational
SDK environments passed pip check; neither is merged into Airflow or Beam.

## Gates and rollback

1. Preserve runtime image/configuration identities and check disk/memory before
   builds. Build sequentially on the existing VM; do not replace containers
   before candidate checks pass or remove the old image to free space.
2. Require actual Python 3.11, exact Airflow/provider or Beam pins, pip check,
   and retained installed-package evidence. Check the DAG imports, ten-task
   graph and relevant operators in the new Airflow image without production
   credentials/metadata writes.
3. Run the actual backfill graphs with DirectRunner on copied source-format
   samples plus one invalid observation per feed. The
   [guarded diagnostic](../../scripts/diagnostics/validate_backfill_runtime.py)
   redirects only the exact known source/sink boundaries. Local mode has no
   cloud IO; cloud mode requires a new labeled runtime_validation namespace,
   whitelisted targets and an isolated GCS prefix. Reconcile five golden
   valid facts and one invalid record per feed, including the NASA leap day.
   Test actual BigQuery/GCS SDK IO separately; never execute fixed production
   WRITE_TRUNCATE destinations. No Dataflow job is part of this validation.
4. Pause an idle DAG for cutover. Change only the captured image selectors and
   recreate Airflow scheduler/triggerer/webserver without restarting PostgreSQL
   or rerunning airflow-init. Preserve exact file preimages and all other
   environment values/settings. Require health/import/metadata identity checks.
5. Apply the isolated Terraform runtime change for the consumer and verify all
   four serving runtime IDs/source files. Require a complete controlled daily
   run with all ten tasks, live timestamp provenance, thirteen data gates,
   forty valid forecasts and BI source checks. Restored scheduling and final
   VM shutdown outside its operating window are part of the handoff.
6. On failure, reconcile in-flight work, restore the exact prior image selectors
   and old containers, and return the consumer runtime to its captured value if
   needed. Leave acknowledged data and timestamp columns/evidence intact.
   Verify the restored health/source/runtime; do not claim production acceptance
   from an image build or thin unit tests alone.

Generic backfill safety, deployment automation, metadata disaster restoration,
recurring atomic publication and rendered BI acceptance remain separate items.

## Source ownership correction during runtime promotion

The first consumer runtime rebuild served the old 41acd6d seed archive even
though the plan showed only runtime changing. source ignore_changes suppressed
the planned source difference; it did not prevent the build configuration from
resubmitting its configured archive. The explicit serving-byte/AST gate rejected
that result. The DAG remained paused and prior analytics/model snapshots were
preserved. Acceptance requires restoration of the verified 201e9a2 timestamp
package and an audit of any deliveries during the superseded revision.

The consumer build source is now explicitly managed using its immutable object
name/generation. Future application releases must update the source pointer
before applying consumer build changes. Producer source ignores remain a separate
application-deployment ownership limitation for item 32; capture/verify their
actual package before any future build-configuration change.
