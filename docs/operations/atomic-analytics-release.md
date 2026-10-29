# Atomic analytics publication

Checklist item 20, branch `codex/atomic-analytics-release`. This supplies the
publication boundary needed before completing recurring candidate quality gates
in item 15. Deployment acceptance is recorded below after cutover validation.

`analytics_delivery` holds the current release marker, run registry, immutable
application input snapshots, candidate artifacts and recovery events. The nine
Airflow task IDs and all six existing serving table names remain stable. Three
required-feed gates must succeed before the release task can run.

The task binds the exact accepted acquisition IDs/times and generation-pinned
coverage receipts to a logical release ID. It pins the deployed procedure body
SHA256 and uses deterministic BigQuery job IDs. An uncertain response reattaches
to that job. Only a confirmed transient execution failure permits a second job;
permanent validation/permission failures stop the task. Server execution is
bounded to 20 minutes, with a 22-minute task limit and one task retry.

The SQL first atomically captures the curated baseline, exact current staging
acquisitions and input cutoff. A retry uses those saved inputs, including after
late or conflicting staging deliveries. Conflicting frozen source keys fail.
Source coverage/age is still enforced by the existing mandatory feed gates;
more extensive recurring checks remain item 15.

Candidates are computed outside serving tables. Training uses a new
`models.cocoa_release_<release_id>` model; retries reuse that model. The old
`models.cocoa_linreg_40` is preserved as the legacy baseline. Read
`cocoa_related_studio.release_status.model_id` for the active model of record.
Missing training labels or invalid/misaligned forecasts withhold the entire
release. The forecast construction and current historical rescoring retain their
existing meaning; chronological evaluation and model promotion policy remain
items 22-24.

Candidate artifacts and validation metadata are saved before publication. The
SQL then checks that the baseline release and all six serving fingerprints
remain unchanged and replaces all six tables plus the release marker in one
DML transaction. Existing feature/evaluation and BI views consequently read a
consistent query snapshot. Model/table creation happens before that transaction:
[BigQuery transactions](https://docs.cloud.google.com/bigquery/docs/transactions)
support multi-table DML and temporary DDL, but exclude permanent DDL.

The former zero-argument procedure explicitly refuses direct publication. The
replacement freshness view preserves its six columns, corrects the old oil
supplier label to EIA and reads accepted acquisition times from the active
release. Staging activity cannot refresh published-product timestamps. The
verified legacy seed has unknown original acquisition/publication timestamps;
these remain NULL. Report rendering, richer product status and owner acceptance
remain separate BI work.

## Deployment and rollback

Use `scripts/deploy/render_delivery_sql.py` to render only validated project and
dataset identifiers. Delivery, curated and staging datasets must be distinct.
Terraform owns the delivery dataset and scoped Airflow writer grant; application
SQL owns its tables, procedures and data. Create the permanent tables before
daily execution. Seed once from independently fingerprint-verified serving data,
with its actual accepted business date and recovery evidence. Seed and marker
creation are transactional and never infer success from today's calendar.

Deploy the new procedure, rollback procedure and release-linked serving views,
retire the old entrypoint, then install a DAG pinned to the verified routine
body. Preserve the old DAG, procedure/view definitions and model/data recovery
copies before cutover. Artifact/model retention currently preserves all accepted
versions; lifecycle pruning and issued-forecast retention policy remain item 21.

Operator inspection is read-only by default:

```powershell
python scripts/operations/rollback_release.py <target-release-sha256>
```

After diagnosis, explicitly restore a retained accepted target:

```powershell
python scripts/operations/rollback_release.py <target-release-sha256> --apply `
  --expected-current <observed-current-release-sha256> `
  --incident-key '<incident-and-restoration-id>' --reason '<documented reason>'
```

The rollback validates the expected current marker, current serving fingerprints
and all six retained artifact fingerprints before restoring data and the marker
atomically. A stale marker or altered backup aborts. Recovery events and stable
job IDs make uncertain retries idempotent. Model identities stay in the retained
registry; no model is overwritten by rollback. Runtime/source rollback is a
separate maintenance action, with old definitions and pinned source archives.

## Validation

Isolated probe `20261029_845eb43d` used three labelled disposable datasets.
Failures after freeze, after candidate construction and after the first serving
write left all six fingerprints unchanged. Late conflicting staging did not
change a retry's frozen inputs. Publication succeeded; completed retry preserved
outputs, publication time and model ETag. A changed frozen request was rejected.
Rollback restored all six baseline tables, rejected a stale current marker,
rejected an old-base candidate after rollback, and rejected a deliberately
altered retained forecast. Restoring the original artifact permitted recovery
to the accepted candidate. Freshness followed the active release and did not
invent legacy timestamps.

The real Python 3.11.10/Airflow 2.10.2 image with BigQuery client 3.20.1 parsed the
nine-task candidate DAG and executed the actual job adapter against the isolated
accepted release. Repeat attached to the same job and preserved its model.
Three temporary dataset writer grants were removed afterward. The native check
also caught omitted null values in GET-job metadata; insertion now restores an
explicit typed NULL, covered by a regression test.

The regular October 29 scheduled production run accepted cocoa/oil and rejected
weather before SQL. This is evidence of withholding; it is not a fresh compliant
release. Native fixtures prove publication mechanics, not supplier availability,
chronological predictive accuracy or rendered BI behavior. Production cutover,
cleanup, exact hashes and final check counts are still pending.
