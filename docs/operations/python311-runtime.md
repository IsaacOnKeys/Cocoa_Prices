# Python 3.11 runtime standardization

Owner requirement added 2026-10-28 under checklist item 31. Branch:
codex/python311-runtime. Status: deployed and accepted after image, native-runner/cloud IO, complete
production, cold-start and final operator-state verification. This is a bounded
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

## Verified build and production receipt

Build source: 81c665fd6fcb22ca7dcc47a745a35d6cb08d87cf. Consumer source-ownership
correction and operational dependency manifest: e630966. The protected source
bundle is under gs://raw-historic-data/operations/2026-10-28/python311-81c665f/,
source.zip generation 1793201768449231, SHA-256
`ecbe271dd40a244713892e4841f787008d24e54e32b2320070fa17f242b803e4`.

| Component | Verified runtime | Retained image identity |
| --- | --- | --- |
| Airflow webserver/scheduler/triggerer | Python 3.11.10, Airflow 2.10.2, Google provider 10.22.0, Pendulum 3.0.0 | cocoa-airflow:2.10.2-python3.11-81c665f; sha256:c1b32d5fc80eac68a32787a77f424a7e6eb82931f0c4d4e2e81c33cd8ff3737e |
| Beam backfill/client | Python 3.11.18, Beam 2.65.0; existing NumPy/pandas/gRPC/protobuf pins | cocoa-backfill:python3.11-81c665f; sha256:eadba330e47337def05fbbe620cb23efd66001a51da56ebdc5d0382fb884e529 |
| All four active functions | python311; every serving archive member matches the repository | Consumer revision stage-analytics-data-00007-buy; checksum-verified 201e9a2 timestamp package |
| Development and cloud-control environments | Python 3.11.9, separate pinned dependency sets, pip check passed | .build/python311-dev and .build/python311-ops; generated local tools, not shipped source |

Both images built and passed pip check; full installed-package evidence is
captured in the private runtime receipt with hashes. The isolated Airflow image
initialized only disposable SQLite, parsed the real ten-task DAG and retained
its schedule/timezone/provider operator. No production metadata or credentials
were mounted into that test.

All three actual backfill graphs then ran with DirectRunner on source-format
samples plus one invalid record each. Network-disabled local execution produced
five golden valid facts and one rejected record per feed, including the NASA
leap day. Actual BigQuery/GCS execution matched those same outputs in
runtime_validation_20261028_81227170: six successful load jobs and six isolated
tables. The VM identity initially had only temp-bucket read access. A narrowly
scoped object grant for that exact test prefix, expiring at 18:00 UTC, enabled
the upload/read/delete path; the grant and all test tables/objects were removed
after success. No project-wide IAM grant or production backfill write was made.

Before cutover, the idle DAG was paused and six source/output snapshots plus a
model copy were retained in
`cocoa-prices-430315.python311_recovery_20261028_81227170`. The copied model
reproduced all forty saved forecasts within 1e-8. Restoring those products is
an operator-only DML transaction; acknowledged staging evidence is retained.

Only the two image selectors changed in live Compose/.env. Rendered non-image
settings, critical container environment and mounts matched the captured runtime.
PostgreSQL was not restarted during container replacement and retained its ID and
mounts. The new services passed health/import checks and could decrypt the needed
metadata connection. The old image and private original files remain available.

The serving-source gate caught the initial runtime rebuild's stale 41acd6d seed
archive. It was corrected to the exact 201e9a2 package with explicit Terraform
source ownership. A complete, timestamp-bounded, paginated audit found zero
accepted application events and zero successful POST requests on superseded
revision stage-analytics-data-00006-yay. No production deliveries needed repair.
The pinned input archive is checksum-verified and protected by an object-scoped
temporary hold; authorized administrators can release that hold.

Controlled run `python311_runtime__e630966__20261028` passed all ten tasks from
16:50:08.293568 to 16:51:16.156682 UTC on 2026-10-28. Its procedure job was
`airflow_cocoa_daily_run_bq_pipeline_2026_10_28T16_50_07_00_00_7e9d1dde47cf15a444a1d79b12d85ec0`.
Two cocoa, five oil and eight weather rows had zero acquisition/receipt/raw-time
errors and matched curated values. Thirteen data checks passed; joined/features/
prediction history have 4,683 unique rows through October 28. All forty forecasts
for October 29 through December 7 are unique, finite and positive. Three BI source
checks contain no missing outputs. SQL routine identity is unchanged. This is
runtime/data-delivery evidence, not forecasting-accuracy or rendered-report
acceptance.

All 203 credential-free tests, Ruff, 34 Python syntax checks, five Bash syntax
checks, changed-document links and deterministic fresh-directory packaging passed
on Python 3.11. CI now selects only 3.11; no new hosted CI run is claimed.
Dated earlier 3.12 receipts and rollback artifacts remain truthful history.

## Final startup and operator state

A real VM stop/start selected the Python 3.11 image for initialization,
webserver, scheduler and triggerer. Initialization exited successfully; health,
DAG import, needed connection decryption and all ten persisted successful task
states passed. The PostgreSQL data volume was preserved. All three legacy Beam
units remained disabled/inactive after cold startup. This verifies the normal
startup path and persisted metadata; it is not a disaster-restore exercise.

The VM was restored TERMINATED outside its operating window, with the DAG
unpaused and no active work, verified at approximately 17:14 UTC on 2026-10-28.
The existing 18:30/20:30 Europe/Berlin VM schedule and 19:00 weekday DAG schedule
are unchanged. Both Terraform roots have no drift (93 managed resources).
Retained recovery datasets/model/images and held application artifacts remain
outside Terraform's resource count and are documented in the inventory.

Private held completed_manifest.json records image identities/package hashes,
isolated local/cloud results, source correction and delivery audit, live run/data
checks, recovery artifacts, cold startup and final state: generation
1793207683362392, SHA-256
`1878c44ea759545a3a015da9dfd66df9ed7d95b5cb0bca90481a24166cb8d47c`.
The same prefix retains held airflow-packages.txt and backfill-packages.txt.

The requested Python 3.11 standardization is complete. This does not complete
all remaining integration/failure/publication acceptance criteria of checklist
item 31 or establish the project's overall production readiness.
