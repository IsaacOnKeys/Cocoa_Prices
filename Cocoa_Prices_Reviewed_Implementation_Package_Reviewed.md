# Codex Instructions: Cocoa Repository Refactoring

## 1. Objective and execution scope

When instructed to execute the repository refactoring, follow this file from start to finish. Reorganize the repository around its actual engineering domains while preserving production behavior. Make the smallest structural and compatibility changes needed for that result.

If the user asks only to edit these instructions or create a Git checkpoint, complete only that request. Do not start the migration merely because this file contains implementation instructions.

Keep these categories separate:

- **A - Structural:** move, rename and organize source files.
- **B - Compatibility:** update paths, imports, packaging, build contexts and deployment mappings required by A.
- **C - Engineering:** change behavior, dependencies, models, infrastructure or reliability. Record these tasks in the backlog; do not implement them in this migration.

Read the entire instruction file, inspect the current repository state and follow the component gates below. Do not recreate the earlier SQL-fragment/renderer proposal or archive obsolete runtime configuration under documentation.

## 2. Verify the Git checkpoint and protect the baseline

### 2.1 Use the refactoring-start checkpoint

Use the annotated local tag `refactor-start-2026-10-26` to identify the start of this refactoring project. Resolve it to its commit before comparing changes:

```bash
git rev-parse refactor-start-2026-10-26^{commit}
git show --stat refactor-start-2026-10-26
```

Preserve the checkpoint and its history. Do not move, overwrite or delete the tag. Do not push it without a user instruction.

Treat the checkpoint as the repository state before application refactoring: it includes this instruction file and preserves the existing diagram edits/deletions in a separate preceding commit. Distinguish those existing diagram changes from later migration work. Do not recreate their deleted files merely to match the old inventory.

Do not assume Git captures ignored `.env`, `.tools/`, Terraform caches or private `.tfvars` files. Inspect only the metadata needed to protect those local files; do not stage or expose their sensitive contents.

### 2.2 Re-record the current Git state

Before changing application files:

1. Record the current branch, full HEAD SHA, configured upstream and tracked-file inventory.
2. Inspect staged, unstaged, untracked and relevant ignored paths. Protect changes made after the checkpoint.
3. Compare current changes with the checkpoint. Separate user work from migration-owned edits.
4. If a path overlaps unresolved user work, defer that operation and continue only with independent changes.
5. Preserve binary contents and deletions accurately. Do not rely on a text diff alone for binary recovery.
6. Do not automatically commit, stash, reset, restore or discard user work. The initial checkpoint request does not authorize further migration commits.

Re-verify branch relationships rather than trusting old snapshots. The pre-checkpoint source commit was `e12efdd87598e865a9a3b523b4efb75f2fa6c340`; local `main`, cached `origin/main` and `Terra` pointed there. Cached `origin/master` pointed to the older `0a30dcecb18cd2bfe50efdee7242c896566ce5dc`, and cached `origin/HEAD` pointed to `origin/master`.

Treat those values as historical local observations, not a live remote check or proof of the current server default branch. Do not fetch, rename branches, reset, merge, push or change the default branch merely to reconcile an old claim about public `master`.

### 2.3 Preserve SQL bytes

Move `run_full_pipeline.sql` to `sql/procedures/run_daily_pipeline.sql` with identical bytes. Record and compare SHA-256 hashes before and after movement. Do not normalize line endings, format SQL, substitute semantic equivalence for byte identity, split executable SQL or introduce a renderer.

Expose feature engineering through `docs/feature-engineering/README.md`, `sql/README.md` and README links to the single canonical procedure. Document stage boundaries without creating another maintained executable definition.

### 2.4 Establish external runtime mappings

Before changing a path-sensitive component, establish its real deployment/install mapping. Capture the relevant systemd units/drop-ins, Airflow Compose and mounts, interpreter/virtual-environment paths, environment-file locations, installed component versions and function deployment mechanics.

Inspect potentially sensitive configuration privately. Retain only safely sanitized material in Git or reports. Do not dump environment contents, print secret values, copy credentials into documentation, execute Fernet installers or rotate keys. Record secret references and ownership instead of payloads.

Preserve production `/opt/beam/...` and `/opt/airflow/...` paths. Map a renamed repository source to the unchanged installed filename when appropriate. If a VM executes directly from the checkout or a live bind mount uses that path, block the affected move until the runtime dependency is resolved.

Defer only the affected component and dependent work when configuration is unknown. Continue independent hygiene, documentation or safely validated source changes.

### 2.5 Require Terraform baseline plans

Relocate neither Terraform root until both main and bootstrap roots have verified backend identities and successful no-change baseline plans. Missing credentials, unavailable backends, errors or proposed changes defer relocation. Do not treat validation alone as sufficient.

For each root, record backend bucket/prefix, workspace, CLI/provider versions, lockfile identity and a sanitized plan summary. With `terraform plan -detailed-exitcode`, accept exit 0 only; treat exit 1 as an error and exit 2 as proposed changes. Review the summary as well. Do not print sensitive values or commit plan artifacts.

Preserve these backend identities:

```text
bucket: cocoa-prices-430315-terraform-state
main prefix: terraform/main
bootstrap prefix: terraform/bootstrap
```

Do not mutate Terraform state, change workspaces/backends, re-import resources or apply plans. Preserve both lockfiles. Reinitialize directory caches against the same backends without moving `.terraform/` caches or upgrading providers.

### 2.6 Resolve Docker compatibility with the cleaner moves

Determine the root Dockerfile's supported usage before moving scripts named in its `COPY` instruction. Do not leave a broken recipe between phases.

If supported, move the Dockerfile with the three cleaners to `pipelines/backfill/Dockerfile` in the same validated change. Retain its current source basenames with the matching build context:

```bash
docker build -f pipelines/backfill/Dockerfile pipelines/backfill
```

Update supported build commands and installation mappings while preserving the base image, Beam version, image-internal `/app` paths and entry point. Do not publish images or change production build triggers.

If confirmed unused, delete the Dockerfile with evidence. If usage remains unknown, retain it and defer dependent cleaner moves. Do not move stale runtime configuration into documentation merely to preserve a Docker artifact.

## 3. Target repository structure

Keep the existing checkout name `Cocoa_code/`. Create the following structure only as each component's gates pass. Do not rename the repository or its parent directory. Do not create empty placeholder directories.

```text
Cocoa_code/
??? Cocoa_Prices_Reviewed_Implementation_Package_Reviewed.md
├── .github/
│   └── ISSUE_TEMPLATE/user-story.md
├── airflow/
│   ├── dags/cocoa_daily.py
│   ├── requirements.txt                 # captured component versions only
│   └── docker-compose.yaml              # captured and sanitized only
├── data/
│   ├── README.md
│   ├── samples/
│   │   ├── cocoa_prices.csv
│   │   ├── brent_oil_fred.json
│   │   └── power_daily.csv
│   └── raw/                             # ignored local copies, after archive gate
├── docs/
│   ├── architecture/                    # editable diagrams and selected exports
│   ├── feature-engineering/README.md
│   ├── infrastructure/terraform.md
│   └── operations/runtime-layout.md
├── functions/
│   ├── cocoa/{main.py,requirements.txt}
│   ├── oil/{main.py,requirements.txt}
│   └── weather/{main.py,requirements.txt}
├── infra/
│   ├── terraform/                       # conditional on Terraform gate
│   │   └── bootstrap/
│   └── systemd/                         # actual sanitized captured units only
├── pipelines/
│   ├── requirements.txt
│   ├── streaming/
│   │   ├── cocoa_stream.py
│   │   ├── oil_stream.py
│   │   └── weather_stream.py
│   └── backfill/
│       ├── clean_cocoa_prices.py
│       ├── clean_oil_fred.py
│       ├── clean_precipitation.py
│       └── Dockerfile                   # only if supported; coordinated context
├── schemas/
│   ├── cocoa_schema.avsc
│   ├── oil_schema.avsc
│   └── weather_schema.avsc
├── scripts/
│   ├── deploy/package_functions.py
│   ├── operations/{setup_fernet_secret.sh,restart_with_secrets.sh}
│   ├── development/{stream_start,stream_stop,stream_status}
│   └── diagnostics/check_bucket_access.py
├── sql/
│   ├── procedures/run_daily_pipeline.sql
│   └── README.md
├── tests/unit/                          # meaningful migration checks only
├── .gitignore
├── LICENSE
├── README.md
└── pyproject.toml
```

Leave blocked components at their current paths and show those actual paths in the final report. Leave `terraform/`, `RAW/`, the Dockerfile or consumer source in place when its gate has not passed. Do not represent conditional files as implemented.

Reserve `docs/observability/`, monitoring Terraform under the actual Terraform root, and `infra/ops-agent/` for later engineering work. Do not create those areas without real, authorized content.

## 4. File decisions and compatibility requirements

Follow Appendix A for all 73 original source paths plus this instruction file. Preserve the seven diagram deletions recorded before the refactoring checkpoint; do not treat those absent paths as new migration deletions. Reconcile the actual checkpoint inventory before changing files. Use this document instead of the earlier SQL-renderer and Docker-archive proposal.

For every operation, update known references in the same change and record external dependencies. Do not interpret wildcards as permission to migrate ignored caches or sensitive inputs.

### Functions and contracts

- Keep central schema contents and basenames unchanged.
- Generate each function's self-contained artifact under ignored `.build/`, including `main.py`, its existing `requirements.txt` and its canonical schema beside `main.py`.
- Preserve source file bytes unless an explicitly identified path compatibility change requires otherwise.
- Preserve entry points `publish_price`, `publish_fred_data` and `publish_weather_data`, runtime `python311`, triggers, resources and topics.
- Packaging tests must check archive membership, schema byte/hash equality, entry-point presence without invoking producers, absence of secrets/caches, and repeat-build determinism. Normalize archive metadata if claiming byte-identical archives; do not normalize source contents.
- Remove function-local editable schema duplicates only after these checks pass. Source directories will then require the packaging helper for execution: document local execution from the prepared function root, not raw `functions/<feed>/`.
- Preserve separate function requirements. Do not merge environments or upgrade packages.
- Terraform deliberately ignores function source drift. A local directory move neither deploys code nor establishes equality with the deployed source archive.

### Streaming and backfill paths

- Update the three consumer paths in `stream_start`; resolve the repository root from the helper's own location.
- Keep PID/log behavior and relative schema fallbacks valid. Preserve all environment-variable aliases, target tables and subscription names.
- Keep `/opt/beam/{cocoa,oil,weather}/schemas/*_schema.avsc` defaults and explicit schema-path overrides unchanged. Document the repository-to-install mapping.
- Move shared Beam requirements without changing its contents. Check all three backfill `REQUIREMENTS_FILE` references and supported install/submission commands. Do not activate previously unused configuration or refactor pipeline options.
- Update download examples that currently write `RAW/` only when the data disposition is settled; document any retained path intentionally.
- Preserve historical DataflowRunner support and distinguish it from current DirectRunner streaming. Do not remove a valid backfill reference merely because production streaming uses a VM.

### Airflow and SQL

- Establish the real DAG copy/mount path; retain DAG/task identity and configuration contracts.
- Use the captured Airflow/provider versions and representative configuration to parse the DAG without cloud calls. Do not invoke producer tasks or execute BigQuery jobs as a parse check.
- The DAG calls a deployed procedure by FQN and does not read the SQL source filename. A SQL source move does not require changing that call or deploying the procedure.
- Preserve all SQL bytes and BigQuery object identities. Feature documentation must distinguish existing historical predictions/MAPE from rigorous chronological out-of-sample evaluation.

### Terraform, scripts and documentation

- After an approved Terraform move, schema references in `pubsub.tf` become `${path.module}/../../schemas/<feed>_schema.avsc`.
- Preserve both root lockfiles. Move private `terraform.tfvars` locally only after its ignore protection is verified; never stage it.
- Consolidate Terraform ignore rules only when equivalent root protections are demonstrated. If Terraform stays in place, keep its local ignore file unless a separately validated consolidation is useful.
- The two Fernet files are installer snippets that create helpers under `/opt/airflow/bin`; moving their repository copies must not change installed destinations or execute them. Defer consolidation of repeated helper bodies.
- Move `docs/terraform-infrastructure.md` to `docs/infrastructure/terraform.md`; update root README and Terraform README links, including relative depth after relocation.
- Retain both editable application diagram layouts. Resolve the existing diagram deletions before choosing Terraform graph sources/exports; do not automatically resurrect or absorb user changes.
- Allow old paths only in explicitly documented historical context, baseline records or intentional compatibility mappings. A preserved installed filename is not a stale reference to replace blindly.

## 5. Migration phases

For each phase, record its baseline, scoped changes, applicable checks and a reversible record of your changes. If a required check fails or is unavailable, leave the affected operation unapplied or safely reverse only your own changes. Never use blanket resets, cleanup or restoration that could overwrite user work. Continue independent phases only when their prerequisites still hold.

### Phase 0 — Baseline and component gates

1. Re-record branch, HEAD, upstream, remote-reference freshness, tracked inventory, status and diff names. Do not claim a live remote comparison without one.
2. Verify the start checkpoint, preserve the separately recorded diagram changes and protect any subsequent user edits. Defer overlapping edits until their intended disposition is known.
3. Capture sanitized runtime/install mappings for functions, consumers and Airflow before their path-sensitive moves.
4. Establish SQL and schema content hashes.
5. Establish Terraform backend/workspace identities and no-change plans for both roots if relocation is to proceed.
6. Resolve Docker use/build context before the cleaner moves. Establish archive availability before raw-data removal.

Record each component as ready or deferred. Preserve the existing start checkpoint. Do not make further commits, tags, pushes, stashes or cloud mutations without a separate user instruction.

### Phase 1 — Hygiene and guardrails

- Remove tracked egg metadata only after confirming it is generated and unreferenced.
- Expand `.gitignore` for Python caches, pytest/Ruff caches, generated function builds and Terraform private/runtime files. Preserve existing protections.
- Add minimal `pyproject.toml` tool configuration. Do not invent distribution name/version/build metadata or impose one unsupported Python version across independent environments.
- Preserve the existing Ruff rule scope initially. Configure pytest discovery under `tests/` so it does not import the authenticated bucket diagnostic.
- Add only meaningful schema/package/path migration checks, with the required tools available in an appropriate development environment. Do not add runtime dependency upgrades to make checks pass.

Validate tool configuration, Python syntax and ignore behavior before continuing dependent work.

### Phase 2 — Canonical schemas and function packaging

- Build and test the packaging helper against the existing layout before removing schema copies.
- Move producer source/requirements to `functions/{cocoa,oil,weather}/` and update the helper to those final paths in the same validated change.
- Compare generated schema bytes and preserved requirements/entry points; document local execution from generated roots.
- Remove independent schema copies only after package checks pass.
- Do not deploy functions, upload archives or update Terraform source generations.

### Phase 3 — Beam pipelines and coordinated Docker compatibility

- Move shared requirements and historical cleaners only when their build/deployment mappings are known.
- Resolve active Dockerfile relocation/build context with the cleaners in the same change. Confirmed-unused deletion is allowed only under implementation authorization; unknown usage defers the dependent moves.
- Rename streaming source files to `pipelines/streaming/*_stream.py` only when installed-path mappings are established.
- Move development helpers and update repository-relative paths, preserving current PID/log semantics.
- Move the bucket diagnostic to `scripts/diagnostics/check_bucket_access.py`; do not execute it as a test.
- Preserve installed VM paths, runners, options, dependencies, data destinations and service lifecycle behavior.

Compile without importing pipeline modules, run migration checks, verify path resolution, and perform shell syntax checks. Build the retained Docker recipe when its required environment is available; do not publish/run it against production. Defer affected changes when required checks are unavailable.

### Phase 4 — Airflow

- Move the DAG to `airflow/dags/cocoa_daily.py` only when its external mapping is understood and baseline parsing can be validated.
- Preserve DAG ID, task IDs, schedule, timezone, Variables, XCom keys, trigger rules, `gcp_default` and the procedure FQN.
- Add captured Compose/configuration and an appropriate component dependency record only if safely sanitized. Do not commit a wholesale VM package dump.
- Compare baseline and moved DAG structure using the captured runtime; do not deploy or change the production mount.

### Phase 5 — Intact SQL and feature visibility

- Move the SQL file intact to `sql/procedures/run_daily_pipeline.sql` and require matching pre/post SHA-256 hashes.
- Document staging deduplication/MERGE, calendar/join/forward fill, feature engineering, training, 40-day prediction, historical predictions and MAPE in `sql/README.md`.
- Document existing cocoa lags at 1/7/28 days; seven-row rolling averages for oil, precipitation and soil moisture; weekday/month features; and `LEAD(y, 40)` target construction in `docs/feature-engineering/README.md`.
- Explain the current horizon in relation to the daily calendar and document current evaluation limitations without changing them.
- Do not add a renderer, template, executable fragments or production SQL validation/deployment jobs.

### Phase 6 — Infrastructure

- If both baseline plans pass, move both Terraform roots under `infra/terraform/`, preserving lockfiles and all resource/state identities.
- Update only required schema paths, private-input location and documentation/working-directory references.
- Reinitialize caches against the same backends without migration or provider upgrades. Compare formatting, validation and both post-move no-change plans.
- If a plan fails or proposes changes, stop the affected migration, report a sanitized summary, and safely return only migration-owned changes to their known-good state where possible. Never apply the delta.
- If baseline checks are unavailable, leave Terraform and dependent references at their current paths.
- Add only real sanitized systemd units/drop-ins that have been captured. Do not install, enable, disable or restart services.

### Phase 7 — Raw-data disposition

- For each full snapshot, verify the exact archived contents, durable GCS location, provenance, checksum and recovery before untracking or removing it.
- If the archive is missing, leave the snapshot tracked and record a separate archival task; this repository-only migration does not authorize creating or modifying cloud objects.
- Create small representative samples that retain relevant source-format characteristics and document selection/provenance in `data/README.md`.
- After the gate passes, retain useful full local copies under ignored `data/raw/`. Check ignores before untracking; preserve recoverability.
- Do not rewrite Git history. Do not interpret an ignore rule as removal of an already tracked file.

Docker compatibility must already have been settled with Phase 3; this phase does not postpone that prerequisite.

### Phase 8 — Documentation and acceptance

- Move editable architecture sources and selected exports after resolving pre-existing diagram changes.
- Update README only after applied and deferred paths are known. Link valid current files, not the unconditional target tree.
- Document actual runtime ownership/install paths and the separate scheduling roles: Airflow orchestrates ingestion/SQL; Cloud Scheduler starts/stops the VM.
- Distinguish current VM streaming from historical Dataflow backfills. Do not claim DLQ routing, monitoring or reproducible deployment merely because topics, IAM or infrastructure resources exist.
- Preserve historical validation dates. Do not describe earlier static checks as newly run or as proof of production behavior.
- Run applicable acceptance checks and provide the report in Section 10. Report a partial migration when any required component remains deferred.

## 6. Protected interfaces and prohibited actions

| Area | Protection |
|---|---|
| User work and Git history | Preserve existing edits/deletions; no automatic commits, stashes, resets, history rewrite or pushes. |
| Airflow | Preserve DAG `cocoa_daily`, task IDs, schedule, timezone, trigger rules, Variables, XCom keys and `gcp_default`. |
| BigQuery | Preserve `cocoa-prices-430315.cocoa_related.run_daily_pipeline` and existing table/view/model names. |
| SQL and ML | Preserve SQL bytes, feature expressions, target construction, model options, forecasts and current MAPE definition. |
| Functions | Preserve entry points, Python runtime, triggers, resource/topic identities, dependencies and secret lookup behavior. |
| Streaming | Preserve runners/options, subscription/table destinations, environment aliases and schema contents. |
| VM runtime | Preserve installed files, `/opt/beam/...`, `/opt/airflow/...`, virtual environments and service behavior. |
| Secrets | Do not expose, commit, rotate or change payloads; sanitize captured configuration. |
| Terraform | Preserve backends, workspaces, resource addresses/names, variables, outputs, locks and ownership boundaries. |
| Raw data | Preserve authoritative snapshots until archive/provenance/checksum/recovery gates pass. |
| Cloud resources | No function deployment, image publishing, object upload, Terraform apply/state mutation, systemd operations, production Airflow change or BigQuery procedure deployment. |

Use only appropriate read access for inventory and plan checks. Do not run production backfills, ingestion tasks or authenticated diagnostics as migration validation without separate authorization.

## 7. Validation and acceptance gates

| Gate | Evidence required |
|---|---|
| Git baseline | Exact branch/HEAD/upstream, inventory and preserved user changes; remote freshness stated. |
| Python | In-memory compilation with appropriate component versions; no project imports that initialize clients or pipelines. |
| Tooling | Ruff with no unintended rule expansion; pytest limited to migration checks; relevant checks actually executed. |
| Schemas | Parse and synthetic binary round trips; canonical contents unchanged. |
| Function artifacts | Expected complete runtime files, entry points, byte-identical schemas, no sensitive/generated extras; deterministic packaging if claimed. |
| Beam paths | Development helper resolution, preserved schema overrides/fallbacks and installed-path mappings; backfill requirements/build references verified. |
| Docker, if retained | Supported build context and commands updated with cleaner moves; build checked in the appropriate environment without production execution. |
| Airflow | Baseline and moved DAG parse in captured versions/configuration; DAG/task graph and protected interfaces unchanged. |
| SQL | Identical pre/post SHA-256 hashes; one canonical executable source. |
| Terraform | Same backends/workspaces and locks; `fmt -check`, validation and successful no-change plans for both roots before and after relocation. |
| Shell | `bash -n` in an available Linux-compatible environment; no installer execution. |
| Data | Verified archive contents, provenance, checksum and recovery before untracking; samples parse as documented. |
| References | Old references removed or individually justified as preserved installation mappings, provenance or historical records. |
| Repository | `git diff --check`; complete scoped diff review; no new secrets, state, private inputs, archives or caches staged; existing tracked snapshots retained if archival is blocked. |

Treat skipped or unavailable required checks as unresolved. Defer the affected operation and dependent changes; continue independently verifiable work where safe. Do not claim full completion merely because deferred gates appear in a report.

Do not run `pytest` against the old repository root without constrained discovery: the current bucket diagnostic performs authenticated work at import time. Do not run historical pipelines against production as a smoke test: they use `WRITE_TRUNCATE`.

## 8. Deferred engineering backlog

Record these findings in the final backlog. Do not implement them during structural migration unless the user separately changes the scope.

| Priority | Finding | Later work |
|---|---|---|
| High | Oil producer logs request parameters containing an API key | Remove sensitive logging; assess exposure without dumping logs or rotating secrets in this migration. |
| High | Cocoa source observation date and published date differ | Define and test observation/event-date semantics. |
| High | Historical predictions are not a rigorous chronological out-of-sample backtest | Implement chronological/rolling-origin evaluation and explain it accurately. |
| High | Producer/Airflow failure handling can return or skip | Define feed requirements, retries and alerting. |
| High | Beam decoding failures are printed rather than routed | Add invalid-record handling and recovery. |
| High | DLQ topics do not establish a dead-letter policy | Implement and test actual routing and monitoring. |
| Medium | Airflow can proceed after one feed succeeds | Define partial-feed freshness/completeness policy; verify intended quiet-period behavior. |
| Medium | Backfills overwrite curated tables | Isolate writes and define merge/recovery behavior. |
| Medium | Backfill date limits and parsing/error paths need review | Test real source formats, malformed rows, missing values and date bounds. |
| Medium | Dependencies differ across environments and Docker | Reconcile versions separately, with compatibility testing. |
| Medium | VM metadata, IAM/firewalls and external runtime ownership warrant review | Investigate actual bootstrap behavior and access requirements separately. |
| Medium | Deployment is not fully reproducible from Git | Add controlled application/runtime deployment workflows. |
| Future | No existing automated application test suite | Add unit, contract, integration, data-quality and model-output tests. |
| Future | CI/CD is absent | Add workflows after paths stabilize. |
| Future | Observability configuration is not in source | Add Ops Agent, Logging/Monitoring policies, monitoring Terraform and failure-test evidence. |
| Future | Feature expansion should follow reliable evaluation | Compare temporal/exogenous feature sets using corrected evaluation. |

## 9. Complete components only after their gates pass

For each completed component:

1. Reconcile every original path with the migration matrix and current inventory.
2. Pass applicable checks and preserve protected runtime interfaces.
3. Establish one editable schema source and complete generated function roots if the packaging phase is applied.
4. Make streaming, backfill and orchestration responsibilities visible at their actual paths.
5. Link feature-engineering documentation to the unchanged procedure.
6. If Terraform moved, demonstrate no-change plans for both roots against the same backends.
7. Show applied, deferred and future work accurately in source documentation and the final tree.
8. Preserve user work and confirm that no cloud deployment or runtime mutation occurred.

Report a partial migration when a required component remains deferred. Retain tracked raw snapshots when their archive gate is unresolved. Do not claim runtime equivalence from static checks alone.

## 10. Return a final migration report

When independently authorized work is complete or appropriately deferred, stop and provide:

1. The actual final tree, including retained paths for blocked components.
2. Every file created, moved, renamed, deleted or left under review, reconciled with the checkpoint and Appendix A.
3. Compatibility references updated and intentional old-path/install mappings retained.
4. Validation commands and actual results by component, including unavailable checks.
5. Failed gates, deferred components and unresolved external dependencies.
6. Any detected behavioral difference and how user work was preserved.
7. Confirmation that no production deployment, runtime mutation or unauthorized Git operation occurred.
8. The separate engineering backlog.
9. `git status --short` and a concise scoped diff summary.

Do not commit or push migration changes unless the user explicitly instructs you to do so. Do not create or move additional checkpoint tags without authorization.

## 11. Handle evidence and stop conditions

Re-run only the checks needed for the changed components. Preserve the date and scope of historical results; do not report old checks as newly performed.

Treat the 25 October audit's Python compilation, Ruff, Avro round trips, Terraform formatting/validation and diagram checks as historical static evidence. Re-establish required acceptance evidence for the migration. Do not infer Airflow parsing, deployed-source equality, current no-change plans or production correctness from that audit.

If a required check fails or access is unavailable, stop the affected operation, protect the last known-good state and report the blocker. Safely reverse only your own changes when necessary; never reset the entire worktree. Continue independently authorized work whose prerequisites remain satisfied.

Ask the user only when a missing decision prevents a safe step. Do not invent systemd units, Compose configuration, Terraform, deployment commands or runtime versions to fill gaps.

## Appendix A - Apply the complete file-disposition map

Account for all 73 original tracked paths below and this instruction file. Seven diagram paths were already deleted before the checkpoint; preserve their absence unless a new decision explicitly requires recovery. Re-enumerate the actual tracked set before execution rather than expecting those deleted files to remain tracked.

Apply destinations only when their component gates pass. Treat `REVIEW` as a requirement to establish disposition, not permission to delete an old or absent file.

| Current path | Decision | Destination | Condition / compatibility |
|---|---|---|---|
| `.github/ISSUE_TEMPLATE/user-story.md` | KEEP | Same | Existing issue template; no workflow invented. |
| `.gitignore` | KEEP / UPDATE | Same | Preserve current rules; add proven cache/build/raw/private-input protections. |
| `Dockerfile` | REVIEW | pipelines/backfill/Dockerfile if supported; otherwise confirmed-unused deletion | Resolve before cleaner moves; preserve build context/dependencies; unknown use defers dependent moves. |
| `LICENSE` | KEEP | Same | Preserve license contents. |
| `Project Architecture/cocoa-architecture-tall.xml` | MOVE | `docs/architecture/cocoa-architecture-tall.xml` | Preserve editable source; correct documented names separately from code behavior. |
| `Project Architecture/cocoa-architecture.xml` | MOVE | `docs/architecture/cocoa-architecture.xml` | Preserve editable source; correct documented names separately from code behavior. |
| `RAW/Daily_Prices_Home.csv` | REVIEW | Ignored data/raw/Daily_Prices_Home.csv plus selected sample | Retain tracked in place until exact GCS archive/provenance/checksum/recovery gate passes. |
| `RAW/POWER_Point_Daily.csv` | REVIEW | Ignored data/raw/POWER_Point_Daily.csv plus selected sample | Retain tracked in place until exact GCS archive/provenance/checksum/recovery gate passes. |
| `RAW/brent_oil_fred.json` | REVIEW | Ignored data/raw/brent_oil_fred.json plus selected sample | Retain tracked in place until exact GCS archive/provenance/checksum/recovery gate passes. |
| `README.md` | REPLACE | Same | Describe applied paths and actual architecture; retain dated validation context. |
| `clean_cocoa_prices.py` | MOVE | `pipelines/backfill/clean_cocoa_prices.py` | Gate on external use and Docker compatibility; preserve behavior and requirement references. |
| `clean_oil_fred.py` | MOVE | `pipelines/backfill/clean_oil_fred.py` | Gate on external use and Docker compatibility; preserve behavior and requirement references. |
| `clean_precipitation.py` | MOVE | `pipelines/backfill/clean_precipitation.py` | Gate on external use and Docker compatibility; preserve behavior and requirement references. |
| `cocoa_code.egg-info/PKG-INFO` | DELETE | No tracked replacement | Generated metadata; confirm unreferenced and add ignore rule. |
| `cocoa_code.egg-info/SOURCES.txt` | DELETE | No tracked replacement | Generated metadata; confirm unreferenced and add ignore rule. |
| `cocoa_code.egg-info/dependency_links.txt` | DELETE | No tracked replacement | Generated metadata; confirm unreferenced and add ignore rule. |
| `cocoa_code.egg-info/top_level.txt` | DELETE | No tracked replacement | Generated metadata; confirm unreferenced and add ignore rule. |
| `cocoa_daily_dag.py` | RENAME | `airflow/dags/cocoa_daily.py` | Gate on runtime mapping and baseline/moved DAG parse; preserve protected identity. |
| `cocoa_scraping/cocoa_schema.avsc` | CONSOLIDATE | `schemas/cocoa_schema.avsc` | Remove editable duplicate only after generated package contains byte-identical schema. |
| `cocoa_scraping/main.py` | MOVE | `functions/cocoa/main.py` | Gate on deployment mapping and generated self-contained artifact checks; no deployment. |
| `cocoa_scraping/requirements.txt` | MOVE | `functions/cocoa/requirements.txt` | Gate on deployment mapping and generated self-contained artifact checks; no deployment. |
| `docs/diagrams/terraform-resource-graph-styled-compressed.png` | REVIEW | Pending diagram decision | Preserve the deletion recorded before refactoring; do not restore or consolidate this absent path without a new decision. |
| `docs/diagrams/terraform-resource-graph-styled-transparent.png` | REVIEW | Pending diagram decision | Preserve the deletion recorded before refactoring; do not restore or consolidate this absent path without a new decision. |
| `docs/diagrams/terraform-resource-graph-styled-transparent.svg` | REVIEW | Pending diagram decision | Preserve the deletion recorded before refactoring; do not restore or consolidate this absent path without a new decision. |
| `docs/diagrams/terraform-resource-graph-styled.dot` | REVIEW | Candidate docs/architecture/terraform-resource-graph.dot | Preserve the deletion recorded before refactoring; do not restore or consolidate this absent path without a new decision. |
| `docs/diagrams/terraform-resource-graph-styled.png` | REVIEW | Pending diagram decision | Preserve the deletion recorded before refactoring; do not restore or consolidate this absent path without a new decision. |
| `docs/diagrams/terraform-resource-graph-styled.svg` | REVIEW | Candidate docs/architecture/terraform-resource-graph.svg | Preserve the deletion recorded before refactoring; do not restore or consolidate this absent path without a new decision. |
| `docs/diagrams/terraform-resource-graph.png` | MOVE | `docs/architecture/terraform-resource-graph.png` | Preserve the exact PNG content recorded before refactoring; resolve any later edits before movement. |
| `docs/diagrams/terraform-resource-graph.svg` | REVIEW | Pending diagram decision | Preserve the deletion recorded before refactoring; do not restore or consolidate this absent path without a new decision. |
| `docs/terraform-infrastructure.md` | MOVE | `docs/infrastructure/terraform.md` | Update both README links with actual Terraform location; preserve dated evidence. |
| `fred_oil_prices_api/main.py` | MOVE | `functions/oil/main.py` | Gate on deployment mapping and generated self-contained artifact checks; no deployment. |
| `fred_oil_prices_api/oil_schema.avsc` | CONSOLIDATE | `schemas/oil_schema.avsc` | Remove editable duplicate only after generated package contains byte-identical schema. |
| `fred_oil_prices_api/requirements.txt` | MOVE | `functions/oil/requirements.txt` | Gate on deployment mapping and generated self-contained artifact checks; no deployment. |
| `local_cocoa_stream.py` | RENAME | `pipelines/streaming/cocoa_stream.py` | Gate on VM install mapping; update development helper; preserve installed paths and schema/environment interfaces. |
| `local_oil_stream.py` | RENAME | `pipelines/streaming/oil_stream.py` | Gate on VM install mapping; update development helper; preserve installed paths and schema/environment interfaces. |
| `local_weather_stream.py` | RENAME | `pipelines/streaming/weather_stream.py` | Gate on VM install mapping; update development helper; preserve installed paths and schema/environment interfaces. |
| `requirements.txt` | MOVE | `pipelines/requirements.txt` | Preserve bytes/versions; coordinate install/submission and backfill path references. |
| `restart_with_secrets.sh` | MOVE | `scripts/operations/restart_with_secrets.sh` | Preserve installer/generated destinations; do not execute or rotate secrets. |
| `run_full_pipeline.sql` | RENAME | `sql/procedures/run_daily_pipeline.sql` | Require identical SHA-256; no decomposition, renderer or deployment. |
| `schemas/cocoa_schema.avsc` | KEEP | Same | Canonical editable contract; preserve contents and basename. |
| `schemas/oil_schema.avsc` | KEEP | Same | Canonical editable contract; preserve contents and basename. |
| `schemas/weather_schema.avsc` | KEEP | Same | Canonical editable contract; preserve contents and basename. |
| `setup_fernet_secret.sh` | MOVE | `scripts/operations/setup_fernet_secret.sh` | Preserve installer/generated destinations; do not execute or rotate secrets. |
| `stream_start` | MOVE | `scripts/development/stream_start` | Update root resolution and required script paths; preserve PID/log/status behavior. |
| `stream_status` | MOVE | `scripts/development/stream_status` | Update root resolution and required script paths; preserve PID/log/status behavior. |
| `stream_stop` | MOVE | `scripts/development/stream_stop` | Update root resolution and required script paths; preserve PID/log/status behavior. |
| `terraform/.gitignore` | CONSOLIDATE | `Root .gitignore` | Only after equivalent protection verified; retain when consolidation/move is deferred. |
| `terraform/.terraform.lock.hcl` | MOVE | `infra/terraform/.terraform.lock.hcl` | Retain separate root lockfile unchanged; no provider upgrades; Terraform relocation gate applies. |
| `terraform/README.md` | MOVE | `infra/terraform/README.md` | Terraform gate; update working directories and link to ../../docs/infrastructure/terraform.md. |
| `terraform/bigquery.tf` | MOVE | `infra/terraform/bigquery.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/bootstrap/.terraform.lock.hcl` | MOVE | `infra/terraform/bootstrap/.terraform.lock.hcl` | Retain separate root lockfile unchanged; no provider upgrades; Terraform relocation gate applies. |
| `terraform/bootstrap/main.tf` | MOVE | `infra/terraform/bootstrap/main.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/bootstrap/outputs.tf` | MOVE | `infra/terraform/bootstrap/outputs.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/bootstrap/providers.tf` | MOVE | `infra/terraform/bootstrap/providers.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/bootstrap/variables.tf` | MOVE | `infra/terraform/bootstrap/variables.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/bootstrap/versions.tf` | MOVE | `infra/terraform/bootstrap/versions.tf` | Terraform gate; preserve backend bucket/prefix, required versions and provider requirements. |
| `terraform/compute.tf` | MOVE | `infra/terraform/compute.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/functions.tf` | MOVE | `infra/terraform/functions.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/iam.tf` | MOVE | `infra/terraform/iam.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/main.tf` | MOVE | `infra/terraform/main.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/outputs.tf` | MOVE | `infra/terraform/outputs.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/providers.tf` | MOVE | `infra/terraform/providers.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/pubsub.tf` | MOVE | `infra/terraform/pubsub.tf` | Terraform gate; update three schema references to ${path.module}/../../schemas/<feed>_schema.avsc only. |
| `terraform/scheduler.tf` | MOVE | `infra/terraform/scheduler.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/secrets.tf` | MOVE | `infra/terraform/secrets.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/storage.tf` | MOVE | `infra/terraform/storage.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/terraform.tfvars.example` | MOVE | `infra/terraform/terraform.tfvars.example` | Terraform gate; preserve safe example and private-input instructions. |
| `terraform/variables.tf` | MOVE | `infra/terraform/variables.tf` | Gate on both roots' pre/post no-change plans; preserve resource/state identity and contents except required path references. |
| `terraform/versions.tf` | MOVE | `infra/terraform/versions.tf` | Terraform gate; preserve backend bucket/prefix, required versions and provider requirements. |
| `test_bucket_access.py` | RENAME | `scripts/diagnostics/check_bucket_access.py` | Authenticated diagnostic, not a pytest test; do not execute during validation. |
| `weather_api/main.py` | MOVE | `functions/weather/main.py` | Gate on deployment mapping and generated self-contained artifact checks; no deployment. |
| `weather_api/requirements.txt` | MOVE | `functions/weather/requirements.txt` | Gate on deployment mapping and generated self-contained artifact checks; no deployment. |
| `weather_api/weather_schema.avsc` | CONSOLIDATE | `schemas/weather_schema.avsc` | Remove editable duplicate only after generated package contains byte-identical schema. |
| `Cocoa_Prices_Reviewed_Implementation_Package_Reviewed.md` | KEEP | Same | Preserve this instruction file and the start-checkpoint reference; do not interpret a documentation edit as migration execution. |

### Preserve local, ignored and checkpoint material

| Path/group | Instruction | Constraint |
|---|---|---|
| `.git/` | Keep in place | Preserve Git history and the start checkpoint. |
| `.env` | Keep ignored | Do not print, copy or stage values. |
| `stream_pids.txt` | Retain ignored until usage is known | Do not kill processes or discard an active PID record. |
| `.tools/` and nested files | Keep ignored | Preserve useful local downloads, binaries and support files. |
| `terraform/.terraform/` | Regenerate only if main root moves | Preserve remote backend identity; do not copy cache metadata into source. |
| `terraform/bootstrap/.terraform/` | Regenerate only if bootstrap moves | Keep bootstrap as a separate root. |
| `terraform/terraform.tfvars` | Move locally only with the Terraform root | Verify ignores first; do not display or stage values. |
| Generated `__pycache__/` directories | Ignore; remove only generated cache material when useful | Do not treat caches as application source. |
| This instruction file | Keep tracked at its current path | Preserve the start-checkpoint reference; do not infer migration authorization from a documentation edit. |

Create only the migration-support files required by the applied phases: minimal tool configuration, meaningful checks, function packaging, provenance/samples, feature/runtime documentation, selected diagram exports and captured sanitized runtime configuration. Do not create SQL renderers/fragments, invented runtime files or empty future directories.
