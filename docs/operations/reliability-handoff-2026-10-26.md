# Reliability and CI handoff: 2026-10-26

## Completed repository changes

- Oil producer request parameters, HTTP error bodies, raw response payloads and arbitrary exception messages are no longer logged. Request exceptions are re-raised as sanitized failures without the credential-bearing traceback context.
- Seven canary-key tests exercise success, HTTP/JSON/publish failures, request tracebacks, secret-source fallback and missing credentials. HTTP arguments, Avro payload and publication destination remain tested.
- Deployed oil main.py exactly matches the pre-fix repository bytes (SHA-256 130319fb524fe178820f1336b8e0e1ac96a95b9670f0bf92a61ac1780c482429). Deployed requirements and schema match canonical files. Source-only rollout preparation therefore excludes unrelated drift.
- Current fixed oil main.py SHA-256: `c0cac0a154effeffac551af07a7cc298802ec4fe1ee0ef0fe43f00cc9200e09a`.
- Cocoa processing adopts the captured deployed _to_ts implementation and row mapping, preserving producer ingestion_time rather than replacing it with receipt time. Canonical Avro values remain typed by the schema.
- Weather keeps deployed WEATHER_SUB and WEATHER_TABLE aliases, followed by repository aliases. Existing project/subscription/table/schema overrides remain.
- All three ToBQRow class ASTs match captured deployed versions. Populated/nullable canonical Avro fixtures decode and map to identical rows. Other file differences are documented repository compatibility options.
- Defaults, subscriptions, output tables, DirectRunner/streaming options, installed filenames and canonical SQL/schema bytes are unchanged. No VM consumer code was installed.
- CI is configured on push/pull_request/workflow_dispatch for Python 3.11 and 3.12 with contents:read, non-persisted checkout credentials and commit-pinned actions. It installs only requirements-dev.txt; no GCP secrets or deployments are part of CI.

## Actual verification

- Clean Python 3.11 environment with only pinned development dependencies: 55 tests passed; Ruff passed; 20 Python source files and five shell scripts passed syntax checks.
- Clean Python 3.12 environment with only pinned development dependencies: 55 tests passed; Ruff passed; 20 Python source files and five shell scripts passed syntax checks.
- Tests block network connections. Consumer tests use real fastavro and actual mapper code with fake Beam boundaries, not a real cloud pipeline.
- Source comparisons used checksummed copies retrieved from the VM. The collector restored the VM to stopped; subsequent GCP inspection confirmed TERMINATED. Schedules were not changed.
- GitHub API confirms the public repository default branch is master. No hosted CI run is claimed before publication.
- Publication checks preserve the procedure's verified CRLF working-copy bytes
  through a narrow .gitattributes rule. Its canonical Git blob remains identical
  to the checkpoint. Source-format CSV/JSON sample bytes are also preserved.
- Initial tool approval failures were transient; a normal approved retry succeeded. An initial sandbox-only Bash test failed to launch; elevated checks subsequently passed without skipping the test.

## Remaining release actions

The user explicitly approved publishing the reviewed repository changes through
a branch/PR into master and deploying the tested oil logging fix. The source
and local verification above were prepared before these release actions.

Approved release: publish a review branch and open a PR into master, activating
CI. Deploy only publish_fred_data from its prepared self-contained root;
preserve python311, publish_fred_data entry point and oil-trigger. The inspected
pre-release source generation was 1758719904960528. Hosted CI and deployment
results will be recorded after verification. No consumer installation, secret
rotation or Terraform apply is included in this release.

The prepared oil source uses the existing requirements unchanged; those requirements contain unpinned packages, so a fresh cloud build can resolve newer dependency versions. A source patch alone does not establish deployment/runtime equivalence.

## Resolved machine limitation

C: reached zero free bytes. Only an incomplete test environment and this task's generated pytest scratch directories were removed; user files were not deleted. Clean validation completed using E: temporary environments with bytecode/caches disabled. C: subsequently had approximately 8.5 GB free, allowing workspace writes to resume. gcloud file logging was disabled only for a read-only process to avoid the full disk; saved CLI configuration was not changed.

This handoff has been restored to the repository. The temporary E: copy retains the earlier disk-full status.
