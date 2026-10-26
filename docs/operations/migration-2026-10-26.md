# Partial repository migration: 2026-10-26

This is the historical first-stage snapshot. Later work resolved the listed
structural gates; see the [current portfolio handoff](portfolio-handoff-2026-10-26.md).
Trees, statuses and deferrals below record the earlier stage, not the current layout.

Structural and compatibility work is applied only where gates passed. This is
a partial migration, not proof of production runtime equivalence.

## Baseline and protected user work

- Branch main; HEAD and annotated refactor-start-2026-10-26 checkpoint both resolve
  to 3daf7408aef4a63a78f95d0e0675df335682767e. Configured upstream is origin/main.
- Cached origin/main remains e12efdd87598e865a9a3b523b4efb75f2fa6c340; local main
  is two commits ahead. No fetch occurred. FETCH_HEAD was last written
  2026-08-17 UTC; these cached refs do not establish live remote/default-branch state.
- Checkpoint inventory has 67 tracked paths. Appendix A covers 73 original paths
  plus the instruction file; seven diagram paths were already deleted.
- The preceding 978aecb commit records diagram changes separately. Its retained
  Terraform PNG and both application XML layouts were moved byte for byte.
- The only pre-existing working-tree change was deletion of
  Cocoa_Prices_Reviewed_Implementation_Package_Reviewed.md. That deletion remains
  untouched and is not a migration deletion. The active brief was read from the
  user-supplied Desktop plan; implementation began only after the user's later request.
- Ignored .env, .tools/, stream_pids.txt, Terraform caches and private tfvars remain
  in place. No sensitive payloads were printed or committed.
- No commit, stash, reset, tag change, fetch, push, branch change, or staging occurred.

Ignored local reversal records under .build/migration/ contain baseline.json,
working-file copies in original/, operations.json, implementation-plan.md,
samples.json, archive-evidence.json and protected-local.json. The checkpoint
retains original tracked contents, including initial ignore rules. Reverse only
migration-owned paths if needed; do not restore the user's instruction deletion.

## Phase dispositions

| Phase | Applied work / remaining gate |
| --- | --- |
| 0: baseline | Git state, inventory, source hashes, deployment metadata, raw objects and backend/workspace identities recorded. VM and ADC gaps remain. |
| 1: hygiene | Preserve ignore rules; add Python/cache/build/raw protections and minimal tool config. Remove four unreferenced generated egg-info files. No distribution metadata or runtime dependency changes. |
| 2: functions | Build/test helper on original layout, then move main/requirements files, rebuild/test final layout and remove three identical editable schema duplicates. |
| 3: Beam/Docker | Move/test development helpers and authenticated diagnostic. Keep consumers, cleaners, shared requirements and Dockerfile at original paths. |
| 4: Airflow | Deferred: live copy/mount mapping and captured-runtime baseline DAG parse unavailable. No guessed Compose or requirements file. |
| 5: SQL | Move intact procedure and document existing features/stages/evaluation. No SQL execution or deployment. |
| 6: infrastructure | Both baseline plans fail ADC refresh with invalid_grant, exit 1. Neither Terraform root/cache/private input moves. Documentation and byte-preserved installer snippets moved; no invented systemd units. |
| 7: data | Three representative samples/provenance added; all full RAW snapshots remain tracked. |
| 8: acceptance | Applied layout, full matrix, actual validation, deferrals and separate engineering backlog documented. |

## Actual final source tree

Ignored local tools, private configuration, caches, logs/PID files and .build/
remain local and are excluded from this tree.

~~~text
Cocoa_code/
+-- .github/
|   +-- ISSUE_TEMPLATE/
|       +-- user-story.md
+-- RAW/
|   +-- Daily_Prices_Home.csv
|   +-- POWER_Point_Daily.csv
|   +-- brent_oil_fred.json
+-- data/
|   +-- samples/
|   |   +-- brent_oil_fred.json
|   |   +-- cocoa_prices.csv
|   |   +-- power_daily.csv
|   +-- README.md
+-- docs/
|   +-- architecture/
|   |   +-- cocoa-architecture-tall.xml
|   |   +-- cocoa-architecture.xml
|   |   +-- terraform-resource-graph.png
|   +-- feature-engineering/
|   |   +-- README.md
|   +-- infrastructure/
|   |   +-- terraform.md
|   +-- operations/
|       +-- engineering-backlog.md
|       +-- migration-2026-10-26.md
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
|   +-- procedures/
|   |   +-- run_daily_pipeline.sql
|   +-- README.md
+-- terraform/
|   +-- bootstrap/
|   |   +-- .terraform.lock.hcl
|   |   +-- main.tf
|   |   +-- outputs.tf
|   |   +-- providers.tf
|   |   +-- variables.tf
|   |   +-- versions.tf
|   +-- .gitignore
|   +-- .terraform.lock.hcl
|   +-- README.md
|   +-- bigquery.tf
|   +-- compute.tf
|   +-- functions.tf
|   +-- iam.tf
|   +-- main.tf
|   +-- outputs.tf
|   +-- providers.tf
|   +-- pubsub.tf
|   +-- scheduler.tf
|   +-- secrets.tf
|   +-- storage.tf
|   +-- terraform.tfvars.example
|   +-- variables.tf
|   +-- versions.tf
+-- tests/
|   +-- unit/
|       +-- test_development_helpers.py
|       +-- test_function_packages.py
|       +-- test_schemas.py
+-- .gitignore
+-- Dockerfile
+-- LICENSE
+-- README.md
+-- clean_cocoa_prices.py
+-- clean_oil_fred.py
+-- clean_precipitation.py
+-- cocoa_daily_dag.py
+-- local_cocoa_stream.py
+-- local_oil_stream.py
+-- local_weather_stream.py
+-- pyproject.toml
+-- requirements.txt
~~~

## Complete original-path disposition matrix

All 74 Appendix A paths are accounted for. Old paths below are explicit
baseline/historical records, not executable references to replace blindly.

| Original path | Disposition | Actual destination / condition |
| --- | --- | --- |
| `.github/ISSUE_TEMPLATE/user-story.md` | KEEP, unchanged | Existing content preserved |
| `.gitignore` | UPDATE | Existing rules plus generated-artifact protections |
| `Dockerfile` | DEFER, unchanged | External build/deployment use unresolved; retain valid Docker COPY and backfill requirements references |
| `LICENSE` | KEEP, unchanged | Existing content preserved |
| `Project Architecture/cocoa-architecture-tall.xml` | MOVE, identical bytes | `docs/architecture/cocoa-architecture-tall.xml` |
| `Project Architecture/cocoa-architecture.xml` | MOVE, identical bytes | `docs/architecture/cocoa-architecture.xml` |
| `RAW/Daily_Prices_Home.csv` | RETAIN, tracked | Full archive/provenance/checksum/recovery gate incomplete |
| `RAW/POWER_Point_Daily.csv` | RETAIN, tracked | Full archive/provenance/checksum/recovery gate incomplete |
| `RAW/brent_oil_fred.json` | RETAIN, tracked | Full archive/provenance/checksum/recovery gate incomplete |
| `README.md` | UPDATE | Actual paths, architecture, prepared execution roots and partial acceptance |
| `clean_cocoa_prices.py` | DEFER, unchanged | External build/deployment use unresolved; retain valid Docker COPY and backfill requirements references |
| `clean_oil_fred.py` | DEFER, unchanged | External build/deployment use unresolved; retain valid Docker COPY and backfill requirements references |
| `clean_precipitation.py` | DEFER, unchanged | External build/deployment use unresolved; retain valid Docker COPY and backfill requirements references |
| `cocoa_code.egg-info/PKG-INFO` | REMOVE | Unreferenced generated metadata |
| `cocoa_code.egg-info/SOURCES.txt` | REMOVE | Unreferenced generated metadata |
| `cocoa_code.egg-info/dependency_links.txt` | REMOVE | Unreferenced generated metadata |
| `cocoa_code.egg-info/top_level.txt` | REMOVE | Unreferenced generated metadata |
| `cocoa_daily_dag.py` | DEFER, unchanged | Live install/copy/mount mapping unavailable; DAG also needs captured-runtime parse |
| `cocoa_scraping/cocoa_schema.avsc` | REMOVE | Duplicate removed after generated packages/schema checks; canonical schemas/ retained |
| `cocoa_scraping/main.py` | MOVE, identical bytes | `functions/cocoa/main.py` |
| `cocoa_scraping/requirements.txt` | MOVE, identical bytes | `functions/cocoa/requirements.txt` |
| `docs/diagrams/terraform-resource-graph-styled-compressed.png` | ABSENT before checkpoint | Preserve preceding diagram deletion |
| `docs/diagrams/terraform-resource-graph-styled-transparent.png` | ABSENT before checkpoint | Preserve preceding diagram deletion |
| `docs/diagrams/terraform-resource-graph-styled-transparent.svg` | ABSENT before checkpoint | Preserve preceding diagram deletion |
| `docs/diagrams/terraform-resource-graph-styled.dot` | ABSENT before checkpoint | Preserve preceding diagram deletion |
| `docs/diagrams/terraform-resource-graph-styled.png` | ABSENT before checkpoint | Preserve preceding diagram deletion |
| `docs/diagrams/terraform-resource-graph-styled.svg` | ABSENT before checkpoint | Preserve preceding diagram deletion |
| `docs/diagrams/terraform-resource-graph.png` | MOVE, identical bytes | `docs/architecture/terraform-resource-graph.png` |
| `docs/diagrams/terraform-resource-graph.svg` | ABSENT before checkpoint | Preserve preceding diagram deletion |
| `docs/terraform-infrastructure.md` | MOVE + compatibility/documentation edit | `docs/infrastructure/terraform.md` |
| `fred_oil_prices_api/main.py` | MOVE, identical bytes | `functions/oil/main.py` |
| `fred_oil_prices_api/oil_schema.avsc` | REMOVE | Duplicate removed after generated packages/schema checks; canonical schemas/ retained |
| `fred_oil_prices_api/requirements.txt` | MOVE, identical bytes | `functions/oil/requirements.txt` |
| `local_cocoa_stream.py` | DEFER, unchanged | Live install/copy/mount mapping unavailable; DAG also needs captured-runtime parse |
| `local_oil_stream.py` | DEFER, unchanged | Live install/copy/mount mapping unavailable; DAG also needs captured-runtime parse |
| `local_weather_stream.py` | DEFER, unchanged | Live install/copy/mount mapping unavailable; DAG also needs captured-runtime parse |
| `requirements.txt` | DEFER, unchanged | External build/deployment use unresolved; retain valid Docker COPY and backfill requirements references |
| `restart_with_secrets.sh` | MOVE, identical bytes | `scripts/operations/restart_with_secrets.sh` |
| `run_full_pipeline.sql` | MOVE, identical bytes | `sql/procedures/run_daily_pipeline.sql` |
| `schemas/cocoa_schema.avsc` | KEEP, unchanged | Canonical editable contract |
| `schemas/oil_schema.avsc` | KEEP, unchanged | Canonical editable contract |
| `schemas/weather_schema.avsc` | KEEP, unchanged | Canonical editable contract |
| `setup_fernet_secret.sh` | MOVE, identical bytes | `scripts/operations/setup_fernet_secret.sh` |
| `stream_start` | MOVE + compatibility/documentation edit | `scripts/development/stream_start` |
| `stream_status` | MOVE, identical bytes | `scripts/development/stream_status` |
| `stream_stop` | MOVE + compatibility/documentation edit | `scripts/development/stream_stop` |
| `terraform/.gitignore` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/.terraform.lock.hcl` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/README.md` | UPDATE | Infrastructure documentation link only |
| `terraform/bigquery.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/bootstrap/.terraform.lock.hcl` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/bootstrap/main.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/bootstrap/outputs.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/bootstrap/providers.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/bootstrap/variables.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/bootstrap/versions.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/compute.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/functions.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/iam.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/main.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/outputs.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/providers.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/pubsub.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/scheduler.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/secrets.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/storage.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/terraform.tfvars.example` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/variables.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `terraform/versions.tf` | RETAIN | Relocation gate failed; backend, workspace, locks and path-sensitive references unchanged |
| `test_bucket_access.py` | MOVE, identical bytes | `scripts/diagnostics/check_bucket_access.py` |
| `weather_api/main.py` | MOVE, identical bytes | `functions/weather/main.py` |
| `weather_api/requirements.txt` | MOVE, identical bytes | `functions/weather/requirements.txt` |
| `weather_api/weather_schema.avsc` | REMOVE | Duplicate removed after generated packages/schema checks; canonical schemas/ retained |
| `Cocoa_Prices_Reviewed_Implementation_Package_Reviewed.md` | USER DELETION preserved | Already absent before migration; left under review |

## Created files and scoped summary

- `data/README.md`
- `data/samples/brent_oil_fred.json`
- `data/samples/cocoa_prices.csv`
- `data/samples/power_daily.csv`
- `docs/feature-engineering/README.md`
- `docs/operations/engineering-backlog.md`
- `docs/operations/migration-2026-10-26.md`
- `docs/operations/runtime-layout.md`
- `pyproject.toml`
- `scripts/deploy/package_functions.py`
- `sql/README.md`
- `tests/unit/test_development_helpers.py`
- `tests/unit/test_function_packages.py`
- `tests/unit/test_schemas.py`

Scope: 17 moves (14 byte-preserving; three with necessary helper/documentation
edits), seven generated/duplicate removals, three in-place edits and 14 new
support/documentation/sample/test files. The instruction deletion and seven
previously deleted diagram paths are separate user/baseline changes.

## Compatibility and intentional behavior differences

- Function main/requirements bytes, entry points and dependencies are unchanged.
  Canonical editable schemas stay under schemas/. Prepared roots include each
  schema beside main.py. Local execution and future deployment must use the
  prepared root; the packager does not deploy. Raw functions/<feed>/ lacks a schema.
- Documented source-directory deployment and live runtime/trigger metadata
  establish the packaging mapping. Actual oil/weather triggers are oil-trigger
  and weather-trigger, matching unchanged Terraform. Historical data-topic
  deployment notes are outdated. Local artifacts were not compared to deployed archives.
- Start/stop resolve the checkout root from their own location. Running outside
  the checkout now resolves the intended scripts, root PID/log files and relative
  schema fallbacks. Launch order, three PID entries, logging and kill/remove
  semantics remain. Status still lists Python processes. No lifecycle redesign.
- Consumer basenames remain local_*_stream.py intentionally pending live mapping.
  Their /opt/beam defaults, aliases, overrides, runners/options, schemas, tables
  and subscriptions are byte-preserved. Airflow code is unchanged and unparsed.
- Fernet snippets retain their installed /opt/airflow/bin/ destinations and were
  never executed. They are installers, not validation commands.
- The diagnostic retains authenticated import-time work; pytest testpaths now
  excludes it. It was not executed.
- Root/terraform README links now target docs/infrastructure/terraform.md.
  Terraform stays at terraform/, so schema reference depth stays unchanged.
- Docker COPY, three backfill REQUIREMENTS_FILE references, shared requirements
  and RAW/ examples stay valid because those gated files did not move. Historical
  DataflowRunner backfills remain distinct from VM DirectRunner streaming.
- The legacy diagnostic-cache ignore rule is preserved with original protections.
  Old names in source documentation occur only for history, retained components,
  or intentional installed-path mappings.
- One canonical executable SQL file remains; no renderer, fragments, feature/model
  changes or BigQuery deployments were introduced.

See [runtime ownership and mappings](runtime-layout.md) for captured versus
documented evidence, actual trigger/source generations, intended VM paths, and
the remaining sanitized configuration capture.

## Validation actually performed

| Component | Commands / method | Actual result |
| --- | --- | --- |
| Existing layout | py -3.12 -m pytest -q --basetemp=.build/pytest-before; package_functions.py all before source moves | 10 passed; all original-layout runtime roots/ZIPs built before duplicate removal. |
| Final suite | py -3.12 -m pytest -q --basetemp=.build/pytest-after --tb=short | 11 passed: four packaging checks, six nullable/non-null Avro round trips and one stub helper path/PID test. |
| Final artifact generation | package_functions.py all under Python 3.12 and 3.11 | All three roots/ZIPs generated; only allowlisted runtime files. |
| Package acceptance | Membership, main/requirements/schema bytes and SHA-256, AST entry points, private/cache input exclusion, dirty-output refusal, repeat ZIP bytes after changing source mtimes | Passed. ZIP order/time/platform/mode fixed, ZIP_STORED payloads. Source contents unchanged; no producers imported/invoked. |
| Ruff | Baseline/final ruff check --output-format concise .; version 0.9.1 | Passed. Existing default E4/E7/E9/F scope preserved explicitly. |
| Python | In-memory compile of source bytes, no application imports | Python 3.12: 15 source/helper/test files. Python 3.11: three producers before/after move. |
| Shell | Git Bash -n on each of five original/moved shell files | Passed; helper tests used fake Python and fake kill calls in a temporary checkout. |
| Protected bytes | Working-baseline SHA-256 comparison | 53 unchanged or byte-preserved files passed; seven intended removals verified; HEAD/tag and user deletion preserved. |
| Diagrams | Parse both moved XML layouts; compare PNG hash | Passed; no new rendering/visual acceptance claimed. |
| Data | Recover exact GCS generations into temporary local copies; compare working/checkpoint hashes; compare text/JSON privately; parse samples | Three downloads succeeded. Both CSV samples and FRED JSON contain five selected records each. Full sources untouched. |
| Ignores/discovery | git check-ignore for generated/private/cache/raw paths and pytest collection | Expected paths ignored; original Terraform ignore retained; only migration tests collected. |
| References | Local Markdown link resolution; scoped old-path search | Applied links valid; retained/history/install mappings documented individually. |
| Repository | git diff --check; review new-text-file whitespace; staged diff and protected metadata | Passed; no staged changes; ignored private files/PID record retained. |

An initial pytest attempt hit sandbox permissions in the shared system temp
directory. Using an ignored .build/ basetemp resolved the environment error;
package assertions then passed. Bash checks/stubs ran outside the sandbox to
allow Git Bash initialization. No runtime dependencies were installed/upgraded.

SQL pre/post SHA-256:

`bf8d85aa80ff4f58fa08516fc83a35fe454d9e00841ba75f382205959502a43a`

Canonical schema hashes:

- `schemas/cocoa_schema.avsc`: `9016328d69e323ae1ac3e9257e792103479d94abf896e2ae412fbbd208e53c1e`
- `schemas/oil_schema.avsc`: `42aeb2113de5f8a5c519903b9e4b22ad57118a2a99f0cb7dbb55379a1847dc13`
- `schemas/weather_schema.avsc`: `a44700c214dc3950461388bfdbbbb2966a7e335f1817dea255fb4b916bfbba05`


## Failed gates and unavailable checks

| Component | Evidence and outcome | Required before remaining moves |
| --- | --- | --- |
| VM / consumers / DAG | airflow-vm was TERMINATED; ssh airflow-gce timed out. No VM start attempted. | Actual systemd units/drop-ins, installed interpreter/env-file paths/versions, Compose mounts, source copy mapping and captured-runtime baseline/moved DAG parsing without cloud calls. |
| Docker/backfills | Root image installs Beam 2.61.0 and copies cleaners; diagram documents separate Airflow Docker use. Root Dockerfile external use is unknown. | Supported build/submission mapping and appropriate Docker build validation before coordinated moves. No build/publication attempted. |
| Terraform main | workspace show=default; bucket cocoa-prices-430315-terraform-state, prefix terraform/main; plan exit 1 due to invalid_grant during ADC refresh | Reauthenticate ADC and establish no-change baselines for both roots. |
| Terraform bootstrap | workspace show=default; same bucket, prefix terraform/bootstrap; same plan failure/exit 1 | Same gate. Neither root moved. |
| Raw archives | Exact downloads succeeded, but complete provenance/byte/recovery acceptance is unresolved. No configured bucket versioning/retention; soft delete duration=0. | Establish approved exact archive representation and durable recovery evidence; keep full tracked snapshots. No cloud archival writes were made. |

Both baseline commands used plan -detailed-exitcode -input=false -lock=false
-no-color. State locking was disabled; no state or backend mutation occurred.
Terraform CLI is 1.15.8; both Google provider locks pin 7.44.0. Lock hashes:

- `terraform/.terraform.lock.hcl`: `9f764e47ee5d77a60d6df6bf9df5adec7533190486405d53511192827d2692b7`
- `terraform/bootstrap/.terraform.lock.hcl`: `9f764e47ee5d77a60d6df6bf9df5adec7533190486405d53511192827d2692b7`


Private plan logs were not committed or dumped. Failure does not establish a
no-change baseline. No apply, re-import, backend/workspace change, provider
upgrade, root/cache/private-input move, cache reinitialization or post-move
plan occurred. The operator can later run gcloud auth application-default login
in their own terminal, then re-establish both baselines. Normal gcloud CLI
metadata access worked; ADC refresh is a separate failure.

The infrastructure reference's August verification and 25 October static audit
remain dated historical evidence, not current acceptance.

## Raw archive and sample checksums

The owner individually downloaded historical sources, except ICCO was scraped,
then uploaded the files to GCS. Exact original request/retrieval timestamps are
not fully recorded; creation on 2025-10-02 establishes upload timing only.

| Raw object | Generation | Working SHA-256 | Checkpoint Git SHA-256 | Downloaded GCS SHA-256 |
| --- | --- | --- | --- | --- |
| `gs://raw-historic-data/Daily_Prices_Home.csv` | `1759405389658710` | `77a2674c833b8028fdb0f1739f431dac3a3fc450d31d875cdd932d300c00f5ab` | `5b51b24692fd73f686d326b960db84e7199ca6b9978574c3552439801ee6e411` | `77a2674c833b8028fdb0f1739f431dac3a3fc450d31d875cdd932d300c00f5ab` |
| `gs://raw-historic-data/POWER_Point_Daily.csv` | `1759405393897835` | `20df51c47a1de1433b9cd8e3ab9693a62720200435cafdf221fed17f5509461e` | `343800b8580c089dbe8856c96da7821a31ca215bc544c63819e0c024d942680a` | `343800b8580c089dbe8856c96da7821a31ca215bc544c63819e0c024d942680a` |
| `gs://raw-historic-data/brent_oil_fred.json` | `1759405400300622` | `0d9b41d391eefe88afc1cbc5087dbc4e010d5814fd6e1b430a64a93b3e57ce38` | `0efaae1dfc08c84a3d110cd318f23195b7279852bca2e58990ea19545d97a3f6` | `d7c2bbd86df77fdd73ba471f80800327838cc5a8f1f5fbde0be04d513c222d24` |


ICCO matches the checkout; its committed Git text has different line endings.
NASA matches the Git blob; its checkout has CRLF rather than LF. FRED is
byte-different but parsed JSON equals the checkout (3,118 observations). No
full source was reformatted or normalized to force checksum equality.

See [provenance and exact sample selection](../../data/README.md).

| Derived sample | Records | Full working-source SHA-256 | Sample SHA-256 |
| --- | ---: | --- | --- |
| `data/samples/cocoa_prices.csv` | 5 | `77a2674c833b8028fdb0f1739f431dac3a3fc450d31d875cdd932d300c00f5ab` | `de70dec307b3aad3376d82c2dd7358f14420f9d0c903242926a789e8ff0db66b` |
| `data/samples/power_daily.csv` | 5 | `20df51c47a1de1433b9cd8e3ab9693a62720200435cafdf221fed17f5509461e` | `60f9ec03db68a465277d7035795a61e2b73baeeb87b5f025abe1d3def0d4958e` |
| `data/samples/brent_oil_fred.json` | 5 | `0d9b41d391eefe88afc1cbc5087dbc4e010d5814fd6e1b430a64a93b3e57ce38` | `8f1c06b67c444fbb6c53179a1bfb5222ea737f85c6ea36adbaf1119d6be4498d` |

## Final status and boundaries

Actual git status --short follows. Unstaged moves appear as old-path deletions
and untracked destination directories; the matrix reconciles those paths.

~~~text
 M .gitignore
 D Cocoa_Prices_Reviewed_Implementation_Package_Reviewed.md
 D "Project Architecture/cocoa-architecture-tall.xml"
 D "Project Architecture/cocoa-architecture.xml"
 M README.md
 D cocoa_code.egg-info/PKG-INFO
 D cocoa_code.egg-info/SOURCES.txt
 D cocoa_code.egg-info/dependency_links.txt
 D cocoa_code.egg-info/top_level.txt
 D cocoa_scraping/cocoa_schema.avsc
 D cocoa_scraping/main.py
 D cocoa_scraping/requirements.txt
 D docs/diagrams/terraform-resource-graph.png
 D docs/terraform-infrastructure.md
 D fred_oil_prices_api/main.py
 D fred_oil_prices_api/oil_schema.avsc
 D fred_oil_prices_api/requirements.txt
 D restart_with_secrets.sh
 D run_full_pipeline.sql
 D setup_fernet_secret.sh
 D stream_start
 D stream_status
 D stream_stop
 M terraform/README.md
 D test_bucket_access.py
 D weather_api/main.py
 D weather_api/requirements.txt
 D weather_api/weather_schema.avsc
?? data/
?? docs/architecture/
?? docs/feature-engineering/
?? docs/infrastructure/
?? docs/operations/
?? functions/
?? pyproject.toml
?? scripts/
?? sql/
?? tests/
~~~

No production deployment or runtime mutation occurred: no function deploy,
image publish, cloud object upload, BigQuery job/procedure deploy, backfill,
ingestion task, authenticated diagnostic, VM start/stop, systemd operation,
installer execution, Secret Manager payload access/rotation, or Terraform apply/state operation.

The only intended execution differences are repository helper path resolution
and requiring prepared self-contained function roots. Static/stub checks do
not establish live Airflow/Beam execution or production correctness.

All deferred behavior, reliability, evaluation, deployment and observability
work is in the separate [engineering backlog](engineering-backlog.md).
It was not implemented in this structural migration.
