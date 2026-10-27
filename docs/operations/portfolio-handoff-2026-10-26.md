# Portfolio organization and runtime handoff

This records the structural migration snapshot. Subsequent logging fixes,
consumer reconciliation and CI preparation are recorded in the
[reliability handoff](reliability-handoff-2026-10-26.md).

Date: 2026-10-26. The repository is organized for a hiring reviewer, with the
project story and source guide in the [README](../../README.md), domain-based
source directories, credential-free local checks, explicit historical provenance,
and verified infrastructure/runtime evidence.

This completes the structural organization that was previously deferred.
Engineering improvements remain separate, including existing source/runtime
drift in two consumers and rigorous chronological model evaluation.

## What changed and why

- Moved producers, streaming consumers, historical backfills, Airflow DAG,
  infrastructure, operations helpers, SQL and diagrams into visible domains.
- The README now explains the architecture and directs a reviewer to ingestion,
  contracts, orchestration, SQL/ML, backfills, infrastructure and tests.
- Ordered SQL stage directories expose curated data -> feature engineering ->
  model training -> prediction -> evaluation. Each describes its inputs/outputs
  and links to its implementing SQL; the executable procedure remains intact.
- Canonical Avro contracts and generated, deterministic function packages remove
  duplicate editable schemas without changing deployed functions.
- Historical bootstrap is explicitly distinct from daily operational ingestion.
  Protected exact snapshots and a manifest replace full raw files in the
  working source tree; small examples remain available without credentials.
- Existing infrastructure is now verified rather than described as a prototype:
  83 application resources plus the state bucket are already represented in state.
- No speculative CI, monitoring stack, new model behavior, runtime dependency
  reconciliation, or application deployment was added to improve appearances.

## VM and Docker test

The VM was initially stopped. Its unchanged Cloud Scheduler jobs start it
at 18:30 and stop it at 20:30 daily, Europe/Berlin; the DAG runs at 19:00
Monday-Friday. The user explicitly authorized a bounded startup test.

The initial Airflow startup failed because the declared postgres:13 image was
not cached (Docker reported No such image). The same declared image was fetched,
then the unchanged airflow-compose.service was retried. No Compose settings,
database major version, schedules, IAM, or secrets were changed.

Results before shutdown:

- Docker 28.3.3 / Compose v2.39.1; Airflow image apache/airflow:2.10.2.
- All three Beam services were active/running.
- Airflow webserver, scheduler, triggerer and PostgreSQL containers were running.
- Scheduler, triggerer and PostgreSQL container health checks were healthy.
- Webserver /health returned HTTP 200 and healthy metadatabase, scheduler and triggerer.
- The installed DAG and oil consumer match Git after line-ending normalization.
  Installed cocoa/weather consumers have different ASTs from the preserved
  repository sources; no version was silently overwritten or deployed.

The VM was then stopped and GCP confirmed TERMINATED, restoring its initial
state outside the scheduled operating window. No job was manually triggered.
Normal systemd/Compose startup starts the existing consumers and services;
this was not a claim that startup is incapable of processing queued work.
The existing Airflow service obtains its Fernet key normally; the agent did
not capture, print or rotate any secret payload.

The old root Dockerfile is a separate historical backfill recipe. Its base
image, Beam 2.61.0, /app paths and entry point are unchanged. Both original and
final build contexts built successfully in temporary VM directories. The
uniquely tagged review images were removed; no image was published and no
backfill container was run against production. The fetched PostgreSQL image
and normal Docker build cache remain available on the VM.

## Terraform: existing infrastructure adoption

Existing GCS state tracks 83 main-root resource instances and one bootstrap
bucket. Live refreshed plans returned exit 0 with no changes for both roots
before relocation, and again after relocation, cache reinitialization,
fmt -check and validation. No import or apply was needed.

Both roots now live under infra/terraform/. Bucket
cocoa-prices-430315-terraform-state, prefixes terraform/main and
terraform/bootstrap, default workspaces, Terraform 1.15.8, provider 7.44.0,
resource addresses and both lockfiles were preserved. Only required schema
reference depth and source/documentation paths changed.

Initial saved ADC refresh failed. Successful checks used the already working
gcloud session token only in process environment; no token was printed,
persisted in backend cache, or written to source. Saved login state was not changed.
Each root's cache was initialized afresh; old directory caches were not moved.
Private tfvars moved locally only after ignore checks and retain their metadata.

The [GCP ownership map](../infrastructure/gcp-inventory.md) and
[sanitized resource inventory](../infrastructure/gcp-resource-inventory.json)
track every managed address/ID and distinguish service-managed resources,
application-owned SQL/data, runtime files, secret payloads and future work.

## Historical snapshots

Exact existing GCS generations were copied to:

~~~text
gs://raw-historic-data/snapshots/2025-10-02/
  icco/Daily_Prices_Home.csv
  fred/brent_oil_fred.json
  nasa_power/POWER_Point_Daily.csv
  manifest.yaml
~~~

All four new objects have temporary holds. Copies used generation-match=0 to
refuse overwrites. Original root objects and bucket-wide policy were unchanged.
These holds protect against replacement/deletion until explicitly released;
no irreversible retention lock or broader IAM change was made.

The three exact snapshot generations and manifest were downloaded again and
verified byte for byte/SHA-256 before moving full working copies to ignored
data/raw/. The archive uses original GCS bytes, which are the historical
backfill inputs. Git/Windows line endings and FRED JSON formatting differences
were checked for data equivalence and retained in provenance metadata.

The [manifest](../../data/historical_manifest.yaml) records measured record
coverage/counts, source URLs, acquisition method, unknown acquisition dates,
original/new generations, SHA-256 and recovery verification. The date 2025-10-02
is explicitly the original upload date; archival occurred on 2026-10-26.
The archived manifest generation is 1793012710280996.

Backfills now read the corresponding protected snapshot names, preserving their
records, transformations, destinations and WRITE_TRUNCATE behavior. The three
REQUIREMENTS_FILE references resolve to pipelines/requirements.txt without
activating that previously unused setting. Historical DataflowRunner support,
the separate DirectRunner environment and legacy image references are documented.
No backfill or BigQuery job was executed as a check.

Full files are absent from the working source tree; their tracked deletions
await review/commit. Existing Git history was not rewritten. Local caches are
ignored and small source-format samples remain in data/samples/.

## Validation actually performed

| Area | Method | Result |
| --- | --- | --- |
| Migration checks | py -3.12 -m pytest -q --basetemp=.build/pytest-portfolio --tb=short | 14 passed: packaging, nullable/non-null Avro, stub helper/PID behavior, three backfill dependency references. |
| Ruff | ruff check --output-format concise .; unchanged E4/E7/E9/F scope | Passed. |
| Python | In-memory compile; no pipeline/producer imports | 16 source/helper/test files under Python 3.12; producer sources also compiled under Python 3.11. |
| Shell | Git Bash -n on all five helpers/installers | Passed; no installer execution or real process kills. |
| Function artifacts | Original-layout tests/build, final-layout tests/build under Python 3.11/3.12 | Complete allowlisted roots, byte-identical schemas/requirements/source, AST entry points, exclusion of private/cache inputs, deterministic archives. No deployment. |
| Airflow | Network-isolated disposable copy of captured Airflow 2.10.2 / Google provider 10.22.0 / Pendulum 3.0.0 image | Baseline and relocated repository DAG had identical ten-task graphs, schedules, timezone, IDs and trigger rules with a default Variable fixture. No task execution or live Variable-value claims. |
| Docker | Original/final backfill builds; real Airflow startup and read-only /health | Builds passed; startup failure diagnosed/restored; HTTP 200 with healthy service components. |
| Terraform | Two pre-move and two post-move detailed-exitcode plans, lock=false; init -reconfigure -lockfile=readonly, fmt -check, validate | All four plans exit 0, no changes. Both roots validate; backends, workspaces, locks and resource identity unchanged. No apply/state mutation. |
| Byte preservation | SHA-256 versus recorded working baseline | 49 unchanged/moved files preserved, including SQL, canonical schemas, functions/requirements, consumers, DAG, both locks, shared requirements, Dockerfile, original diagrams and local raw representations. |
| Backfill behavior | AST comparison after normalizing only documentation, requirements reference and verified-equivalent snapshot names | All three match; processing/destinations/options otherwise unchanged. |
| Data | Exact generation recovery, SHA-256, normalized-text/parsed-JSON equivalence and sample parsing | Three data snapshots and manifest recovered exactly; three samples parse with five records each. |
| Diagrams | Two XML parses and selected PNG hash | Passed; no new diagram rendering or live production equivalence inferred. |
| References/configuration | Relative Markdown links, scoped old-path review, Compose config --no-interpolate --quiet, redaction assertions | Valid source links/configuration; old names retained only for installed targets, historical records or compatibility mappings. |
| Repository/private files | git diff --check; whitespace review of new authored text; ignores, unchanged HEAD/tag, user deletion and private-file metadata | Passed; no staged changes or unauthorized Git operations. |

The earlier [first-stage report](migration-2026-10-26.md) records its own dated
checks and earlier deferrals; it is historical. The 25 October static audit and
August infrastructure verification remain dated historical evidence.

## Actual final working source tree

Ignored local tools/caches, .env, private tfvars, PID/log files, data/raw/ and
.build/ are excluded from this source tree. Inactive original Terraform cache
directories remain ignored locally and contain no current tracked configuration.

~~~text
Cocoa_code/
+-- .github/
|   +-- ISSUE_TEMPLATE/
|       +-- user-story.md
+-- airflow/
|   +-- dags/
|   |   +-- cocoa_daily.py
|   +-- docker-compose.yaml
|   +-- requirements.txt
+-- data/
|   +-- samples/
|   |   +-- brent_oil_fred.json
|   |   +-- cocoa_prices.csv
|   |   +-- power_daily.csv
|   +-- README.md
|   +-- historical_manifest.yaml
+-- docs/
|   +-- architecture/
|   |   +-- README.md
|   |   +-- cocoa-architecture-tall.xml
|   |   +-- cocoa-architecture.xml
|   |   +-- terraform-resource-graph.png
|   +-- feature-engineering/
|   |   +-- README.md
|   +-- infrastructure/
|   |   +-- gcp-inventory.md
|   |   +-- gcp-resource-inventory.json
|   |   +-- terraform.md
|   +-- operations/
|       +-- engineering-backlog.md
|       +-- migration-2026-10-26.md
|       +-- portfolio-handoff-2026-10-26.md
|       +-- runtime-layout.md
+-- functions/
|   +-- cocoa/
|   |   +-- main.py
|   |   +-- requirements.txt
|   +-- oil/
|   |   +-- main.py
|   |   +-- requirements.txt
|   +-- weather/
|       +-- main.py
|       +-- requirements.txt
+-- infra/
|   +-- systemd/
|   |   +-- airflow-compose.service
|   |   +-- beam-cocoa.service
|   |   +-- beam-oil.service
|   |   +-- beam-weather.service
|   +-- terraform/
|       +-- bootstrap/
|       |   +-- .terraform.lock.hcl
|       |   +-- main.tf
|       |   +-- outputs.tf
|       |   +-- providers.tf
|       |   +-- variables.tf
|       |   +-- versions.tf
|       +-- .gitignore
|       +-- .terraform.lock.hcl
|       +-- README.md
|       +-- bigquery.tf
|       +-- compute.tf
|       +-- functions.tf
|       +-- iam.tf
|       +-- main.tf
|       +-- outputs.tf
|       +-- providers.tf
|       +-- pubsub.tf
|       +-- scheduler.tf
|       +-- secrets.tf
|       +-- storage.tf
|       +-- terraform.tfvars.example
|       +-- variables.tf
|       +-- versions.tf
+-- pipelines/
|   +-- backfill/
|   |   +-- Dockerfile
|   |   +-- README.md
|   |   +-- clean_cocoa_prices.py
|   |   +-- clean_oil_fred.py
|   |   +-- clean_precipitation.py
|   +-- streaming/
|   |   +-- cocoa_stream.py
|   |   +-- oil_stream.py
|   |   +-- weather_stream.py
|   +-- requirements.txt
+-- schemas/
|   +-- cocoa_schema.avsc
|   +-- oil_schema.avsc
|   +-- weather_schema.avsc
+-- scripts/
|   +-- deploy/
|   |   +-- package_functions.py
|   +-- development/
|   |   +-- stream_start
|   |   +-- stream_status
|   |   +-- stream_stop
|   +-- diagnostics/
|   |   +-- check_bucket_access.py
|   +-- operations/
|       +-- restart_with_secrets.sh
|       +-- setup_fernet_secret.sh
+-- sql/
|   +-- 01_curated_data/
|   |   +-- README.md
|   +-- 02_feature_engineering/
|   |   +-- README.md
|   +-- 03_model_training/
|   |   +-- README.md
|   +-- 04_prediction/
|   |   +-- README.md
|   +-- 05_evaluation/
|   |   +-- README.md
|   +-- procedures/
|   |   +-- run_daily_pipeline.sql
|   +-- README.md
+-- tests/
|   +-- unit/
|       +-- test_backfill_paths.py
|       +-- test_development_helpers.py
|       +-- test_function_packages.py
|       +-- test_schemas.py
+-- .gitignore
+-- LICENSE
+-- README.md
+-- pyproject.toml
+-- requirements-dev.txt
~~~

## Complete original-path reconciliation

All 73 original paths plus the instruction file are accounted for. Old names
in this matrix are explicit baseline records, not active source references.

| Original path | Actual disposition |
| --- | --- |
| `.github/ISSUE_TEMPLATE/user-story.md` | Keep unchanged |
| `.gitignore` | Update in place: preserve/add ignore protections |
| `Dockerfile` | `pipelines/backfill/Dockerfile`; identical bytes |
| `LICENSE` | Keep unchanged |
| `Project Architecture/cocoa-architecture-tall.xml` | `docs/architecture/cocoa-architecture-tall.xml`; identical bytes |
| `Project Architecture/cocoa-architecture.xml` | `docs/architecture/cocoa-architecture.xml`; identical bytes |
| `RAW/Daily_Prices_Home.csv` | Protected GCS snapshot + manifest; full checkout representation retained ignored at `data/raw/Daily_Prices_Home.csv`; tracked deletion pending commit |
| `RAW/POWER_Point_Daily.csv` | Protected GCS snapshot + manifest; full checkout representation retained ignored at `data/raw/POWER_Point_Daily.csv`; tracked deletion pending commit |
| `RAW/brent_oil_fred.json` | Protected GCS snapshot + manifest; full checkout representation retained ignored at `data/raw/brent_oil_fred.json`; tracked deletion pending commit |
| `README.md` | Update in place: project-first hiring-review guide and verified claims |
| `clean_cocoa_prices.py` | `pipelines/backfill/clean_cocoa_prices.py`; necessary compatibility/documentation edits |
| `clean_oil_fred.py` | `pipelines/backfill/clean_oil_fred.py`; necessary compatibility/documentation edits |
| `clean_precipitation.py` | `pipelines/backfill/clean_precipitation.py`; necessary compatibility/documentation edits |
| `cocoa_code.egg-info/PKG-INFO` | Remove generated, unreferenced egg metadata |
| `cocoa_code.egg-info/SOURCES.txt` | Remove generated, unreferenced egg metadata |
| `cocoa_code.egg-info/dependency_links.txt` | Remove generated, unreferenced egg metadata |
| `cocoa_code.egg-info/top_level.txt` | Remove generated, unreferenced egg metadata |
| `cocoa_daily_dag.py` | `airflow/dags/cocoa_daily.py`; identical bytes |
| `cocoa_scraping/cocoa_schema.avsc` | Remove duplicate after package checks; canonical schemas/ retained |
| `cocoa_scraping/main.py` | `functions/cocoa/main.py`; identical bytes |
| `cocoa_scraping/requirements.txt` | `functions/cocoa/requirements.txt`; identical bytes |
| `docs/diagrams/terraform-resource-graph-styled-compressed.png` | Already deleted before checkpoint; preserve absence |
| `docs/diagrams/terraform-resource-graph-styled-transparent.png` | Already deleted before checkpoint; preserve absence |
| `docs/diagrams/terraform-resource-graph-styled-transparent.svg` | Already deleted before checkpoint; preserve absence |
| `docs/diagrams/terraform-resource-graph-styled.dot` | Already deleted before checkpoint; preserve absence |
| `docs/diagrams/terraform-resource-graph-styled.png` | Already deleted before checkpoint; preserve absence |
| `docs/diagrams/terraform-resource-graph-styled.svg` | Already deleted before checkpoint; preserve absence |
| `docs/diagrams/terraform-resource-graph.png` | `docs/architecture/terraform-resource-graph.png`; identical bytes |
| `docs/diagrams/terraform-resource-graph.svg` | Already deleted before checkpoint; preserve absence |
| `docs/terraform-infrastructure.md` | `docs/infrastructure/terraform.md`; necessary compatibility/documentation edits |
| `fred_oil_prices_api/main.py` | `functions/oil/main.py`; identical bytes |
| `fred_oil_prices_api/oil_schema.avsc` | Remove duplicate after package checks; canonical schemas/ retained |
| `fred_oil_prices_api/requirements.txt` | `functions/oil/requirements.txt`; identical bytes |
| `local_cocoa_stream.py` | `pipelines/streaming/cocoa_stream.py`; identical bytes |
| `local_oil_stream.py` | `pipelines/streaming/oil_stream.py`; identical bytes |
| `local_weather_stream.py` | `pipelines/streaming/weather_stream.py`; identical bytes |
| `requirements.txt` | `pipelines/requirements.txt`; identical bytes |
| `restart_with_secrets.sh` | `scripts/operations/restart_with_secrets.sh`; identical bytes |
| `run_full_pipeline.sql` | `sql/procedures/run_daily_pipeline.sql`; identical bytes |
| `schemas/cocoa_schema.avsc` | Keep unchanged |
| `schemas/oil_schema.avsc` | Keep unchanged |
| `schemas/weather_schema.avsc` | Keep unchanged |
| `setup_fernet_secret.sh` | `scripts/operations/setup_fernet_secret.sh`; identical bytes |
| `stream_start` | `scripts/development/stream_start`; necessary compatibility/documentation edits |
| `stream_status` | `scripts/development/stream_status`; identical bytes |
| `stream_stop` | `scripts/development/stream_stop`; necessary compatibility/documentation edits |
| `terraform/.gitignore` | `infra/terraform/.gitignore`; identical bytes |
| `terraform/.terraform.lock.hcl` | `infra/terraform/.terraform.lock.hcl`; identical bytes |
| `terraform/README.md` | `infra/terraform/README.md`; necessary compatibility/documentation edits |
| `terraform/bigquery.tf` | `infra/terraform/bigquery.tf`; identical bytes |
| `terraform/bootstrap/.terraform.lock.hcl` | `infra/terraform/bootstrap/.terraform.lock.hcl`; identical bytes |
| `terraform/bootstrap/main.tf` | `infra/terraform/bootstrap/main.tf`; identical bytes |
| `terraform/bootstrap/outputs.tf` | `infra/terraform/bootstrap/outputs.tf`; identical bytes |
| `terraform/bootstrap/providers.tf` | `infra/terraform/bootstrap/providers.tf`; identical bytes |
| `terraform/bootstrap/variables.tf` | `infra/terraform/bootstrap/variables.tf`; identical bytes |
| `terraform/bootstrap/versions.tf` | `infra/terraform/bootstrap/versions.tf`; identical bytes |
| `terraform/compute.tf` | `infra/terraform/compute.tf`; identical bytes |
| `terraform/functions.tf` | `infra/terraform/functions.tf`; identical bytes |
| `terraform/iam.tf` | `infra/terraform/iam.tf`; identical bytes |
| `terraform/main.tf` | `infra/terraform/main.tf`; identical bytes |
| `terraform/outputs.tf` | `infra/terraform/outputs.tf`; identical bytes |
| `terraform/providers.tf` | `infra/terraform/providers.tf`; identical bytes |
| `terraform/pubsub.tf` | `infra/terraform/pubsub.tf`; necessary compatibility/documentation edits |
| `terraform/scheduler.tf` | `infra/terraform/scheduler.tf`; identical bytes |
| `terraform/secrets.tf` | `infra/terraform/secrets.tf`; identical bytes |
| `terraform/storage.tf` | `infra/terraform/storage.tf`; identical bytes |
| `terraform/terraform.tfvars.example` | `infra/terraform/terraform.tfvars.example`; identical bytes |
| `terraform/variables.tf` | `infra/terraform/variables.tf`; identical bytes |
| `terraform/versions.tf` | `infra/terraform/versions.tf`; identical bytes |
| `test_bucket_access.py` | `scripts/diagnostics/check_bucket_access.py`; identical bytes |
| `weather_api/main.py` | `functions/weather/main.py`; identical bytes |
| `weather_api/requirements.txt` | `functions/weather/requirements.txt`; identical bytes |
| `weather_api/weather_schema.avsc` | Remove duplicate after package checks; canonical schemas/ retained |
| `Cocoa_Prices_Reviewed_Implementation_Package_Reviewed.md` | Pre-existing user deletion preserved; not a migration deletion |

## Created support files

- `airflow/docker-compose.yaml`
- `airflow/requirements.txt`
- `data/README.md`
- `data/historical_manifest.yaml`
- `data/samples/brent_oil_fred.json`
- `data/samples/cocoa_prices.csv`
- `data/samples/power_daily.csv`
- `docs/architecture/README.md`
- `docs/feature-engineering/README.md`
- `docs/infrastructure/gcp-inventory.md`
- `docs/infrastructure/gcp-resource-inventory.json`
- `docs/operations/engineering-backlog.md`
- `docs/operations/migration-2026-10-26.md`
- `docs/operations/portfolio-handoff-2026-10-26.md`
- `docs/operations/runtime-layout.md`
- `infra/systemd/airflow-compose.service`
- `infra/systemd/beam-cocoa.service`
- `infra/systemd/beam-oil.service`
- `infra/systemd/beam-weather.service`
- `pipelines/backfill/README.md`
- `pyproject.toml`
- `requirements-dev.txt`
- `scripts/deploy/package_functions.py`
- `sql/01_curated_data/README.md`
- `sql/02_feature_engineering/README.md`
- `sql/03_model_training/README.md`
- `sql/04_prediction/README.md`
- `sql/05_evaluation/README.md`
- `sql/README.md`
- `tests/unit/test_backfill_paths.py`
- `tests/unit/test_development_helpers.py`
- `tests/unit/test_function_packages.py`
- `tests/unit/test_schemas.py`

Scope from the checkpoint: 49 tracked source moves plus three full raw files
archived/retained ignored; seven duplicate/generated removals; two root files
updated; 33 new documentation/configuration/test/sample/support files.
Eight moved files have required compatibility/documentation edits; all other
moved contents retain their baseline bytes. User changes were preserved.

## Git checkpoint, review and boundaries

Branch main, upstream origin/main; HEAD and annotated refactor-start-2026-10-26
both still resolve to 3daf7408aef4a63a78f95d0e0675df335682767e. No fetch was
performed, so cached remote refs are not a live remote comparison. The prior
diagram commit and seven deletions remain intact. The user's deletion of the
old root instruction file remains separate from migration-owned changes.

Changes are local and unstaged. No commit, tag change, stash, reset, push or
history rewrite occurred. GitHub will not show the new layout until an
explicitly authorized commit/push. Local reversal records remain ignored in
.build/migration/; use per-file records rather than overwriting user work.

Cloud/runtime changes explicitly authorized in this follow-up were limited to
the bounded VM start/stop, restoring the declared PostgreSQL image and retrying
the existing Airflow service, disposable Docker tests, and creating/protecting
the four archive objects. No application code deployment, Terraform apply/import,
schedule/IAM change, bucket retention-policy change, image publication, manual
DAG/producer/backfill run, SQL deployment or secret rotation occurred.

Static, graph, package, build and health checks do not establish forecasting
accuracy or end-to-end production equivalence. Remaining behavior/reliability
work, including the two installed consumer differences, is recorded in the
[engineering backlog](engineering-backlog.md).

Actual git status --short (unstaged moves appear as deletions and untracked
destination directories):

~~~text
 M .gitignore
 D Cocoa_Prices_Reviewed_Implementation_Package_Reviewed.md
 D Dockerfile
 D "Project Architecture/cocoa-architecture-tall.xml"
 D "Project Architecture/cocoa-architecture.xml"
 D RAW/Daily_Prices_Home.csv
 D RAW/POWER_Point_Daily.csv
 D RAW/brent_oil_fred.json
 M README.md
 D clean_cocoa_prices.py
 D clean_oil_fred.py
 D clean_precipitation.py
 D cocoa_code.egg-info/PKG-INFO
 D cocoa_code.egg-info/SOURCES.txt
 D cocoa_code.egg-info/dependency_links.txt
 D cocoa_code.egg-info/top_level.txt
 D cocoa_daily_dag.py
 D cocoa_scraping/cocoa_schema.avsc
 D cocoa_scraping/main.py
 D cocoa_scraping/requirements.txt
 D docs/diagrams/terraform-resource-graph.png
 D docs/terraform-infrastructure.md
 D fred_oil_prices_api/main.py
 D fred_oil_prices_api/oil_schema.avsc
 D fred_oil_prices_api/requirements.txt
 D local_cocoa_stream.py
 D local_oil_stream.py
 D local_weather_stream.py
 D requirements.txt
 D restart_with_secrets.sh
 D run_full_pipeline.sql
 D setup_fernet_secret.sh
 D stream_start
 D stream_status
 D stream_stop
 D terraform/.gitignore
 D terraform/.terraform.lock.hcl
 D terraform/README.md
 D terraform/bigquery.tf
 D terraform/bootstrap/.terraform.lock.hcl
 D terraform/bootstrap/main.tf
 D terraform/bootstrap/outputs.tf
 D terraform/bootstrap/providers.tf
 D terraform/bootstrap/variables.tf
 D terraform/bootstrap/versions.tf
 D terraform/compute.tf
 D terraform/functions.tf
 D terraform/iam.tf
 D terraform/main.tf
 D terraform/outputs.tf
 D terraform/providers.tf
 D terraform/pubsub.tf
 D terraform/scheduler.tf
 D terraform/secrets.tf
 D terraform/storage.tf
 D terraform/terraform.tfvars.example
 D terraform/variables.tf
 D terraform/versions.tf
 D test_bucket_access.py
 D weather_api/main.py
 D weather_api/requirements.txt
 D weather_api/weather_schema.avsc
?? airflow/
?? data/
?? docs/architecture/
?? docs/feature-engineering/
?? docs/infrastructure/
?? docs/operations/
?? functions/
?? infra/
?? pipelines/
?? pyproject.toml
?? requirements-dev.txt
?? scripts/
?? sql/
?? tests/
~~~
