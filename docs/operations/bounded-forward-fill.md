# Bounded forward fill and observation age

Checklist item 16, branch `codex/bounded-forward-fill`, implementation `a510f79`,
expiry regression `2b4265b`, deployed pin `4512b32`. This extends the accepted
[recurring quality gates](recurring-quality-gates.md).

The subsequent [publication-provenance correction](deterministic-source-revisions.md#publication-time-gate-correction)
supersedes this receipt's deployed DAG/procedure hashes while preserving its
fill policy and quality-view definition. The results below retain item 16's
original acceptance scope.

## Policy

`bounded_calendar_v1` uses the contracted source-age limits as maximum
calendar-day carry: cocoa 7 days, oil 14, precipitation 7, soil moisture 7.
The limit is inclusive; a cocoa observation on day 0 remains usable through
day 7 and becomes NULL on day 8. Weekends and holidays consume calendar days;
they do not create observations or extend the limit. No invented exchange
holiday calendar is used. Current acquisition/coverage/age gates still apply.

Each measurement carries its own last valid observation date. A source row
with an explicit NULL, weather -999 sentinel or an absent date does not reset
that date. A valid zero rainfall/oil value and finite negative oil remain facts.
Leading history without a valid observation is NULL. This preserves the current
meaning of explicit missingness while bounding the existing fill behavior;
authoritative revision/null-correction precedence remains item 17.

An accepted correction to a past observation recomputes its carry window using
that past observation date, not its acquisition/correction time. Frozen source
candidates and retained per-release source artifacts preserve the original
facts used by each release. This does not add a second revision-selection rule.

The [canonical fill fragment](../../sql/01_curated_data/bounded_join.sql) runs
before features/training. An independent expression in the recurring output
checker rejects values carried past the limit. Expired labels are NULL and are
excluded from training. Rolling means keep their existing seven-row/non-null
semantics; model handling of missing predictors does not refresh source data.
The accepted quality report records the fill-policy version and limits.

## BI fields

[`cocoa_related_studio.ls_series_quality`](../../sql/serving/imputation.sql)
has four rows per accepted analytical calendar date, one per measurement.
It reads retained source and joined artifacts selected by the active release
marker, so rollback changes the data and its provenance together. No new
artifact table, serving-table schema or permanent grant is required.

| Field | Meaning |
| --- | --- |
| `published_value` | Exact value retained in the accepted release |
| `original_value_date`, `age_days` | Last valid source observation date and age on the analytical row date |
| `carry_limit_days`, `policy_version` | Current reporting limits and recorded release policy; older releases are explicitly `legacy_unbounded_or_unknown` |
| `is_imputed` | A present published value carried from an earlier source date |
| `source_row_present`, `source_missing` | Distinguish an absent source date from an explicit missing/sentinel measurement |
| `quality_status` | `observed`, `carried`, `expired`, `unavailable`, `missing` or `inconsistent` |
| `usable_value` | NULL for expired or inconsistent values; suitable for charts that must leave those gaps visible |
| `is_latest` | Filter for the last analytical date in the accepted release |
| `latest_measurement_age_days`, `latest_measurement_overdue` | For latest rows only, actual observation age/status against today's Berlin date |

Use `usable_value` with observation age/quality in historical charts. Filter
`is_latest` and display `latest_measurement_overdue` for current source status;
an old release can have a valid value as of its own date while being overdue now.
The existing release-status view supplies accepted release/publication age.
No claim is made that an external Looker report was visually updated or accepted.

The production read-only preview found 308 expired oil dates and 306 dates for
each weather measurement in the retained October 28 baseline; cocoa had none.
There were no inconsistent source/joined values. New releases blank those
expired historical values. Existing accepted values remain preserved and
identified by the new report fields. At the October 29 preview, weather was
age 7 on the release's date but age 8 today, correctly overdue.

## Validation

`scripts/diagnostics/validate_imputation.py` executes the exact fill fragment
and report view against fixed sparse source facts. It checks inclusive/exceeded
limits, unavailable leading history, weekend carry, zero/negative valid values,
separate weather origins, explicit missingness, sentinel handling, resumed
observations, past revisions, retained prior facts and visible legacy expiry.
The diagnostic only deletes its exact token-named, purpose-labelled dataset.

`scripts/diagnostics/validate_quality.py` adds a deliberately unlimited historical
fill to the recurring fault cases. `validate_atomic_delivery.py prepare
--historical-gaps --receipt <path>` seeds prolonged historical source gaps;
its exercise/rollback phases check actual training/publication, frozen and
completed retries, all three failure points and provenance following rollback.
The synthetic case has exactly 90 expired measurement rows, all NULL after
publication.

Native fixture `20261029_bb167b88` passed all 16 boundary/missingness cases,
current-age fields, source revision/origin retention and legacy expiry. Initial
fixture `20261029_472457e9` passed the original boundary checks and a subsequent
current-age field check. Quality fixture `20261029_85275b3a` passed all 29 cases;
the strengthened unlimited-fill case rejects exactly nine expired dates while
accepting the correct constant carry within the limit. Integrated fixture
`20261029_d9222209` passed actual BQML training/publication, three injected
failure points, frozen/completed retries and guarded rollback/restoration.
All 90 expired values remained NULL, and report provenance followed the active
marker to the legacy baseline and back to the published release.

## Production acceptance, October 29

Controlled run `bounded_fill__a510f79__20261029` ran from
22:38:23.516234 to 22:38:51.913196 UTC. Cocoa and oil passed; NASA precipitation
age 8 exceeded the unchanged 7-day limit. SQL/summary were upstream-failed.
Six serving fingerprints, the accepted marker and legacy model stayed unchanged.
The live 17-field BI view reproduces the preview's historical expiry counts
and correctly marks both accepted weather measurements overdue today.

Installed DAG SHA256:
`815b23d6f1d95c23327fbe9aaf1a067533510e8a082b3bb3994422bfdadc75a8`.
Native-verified procedure-body SHA256:
`afb59def7188cbc4e7e19c63825b5ab9b647c388cdca8b02647a54eb8ba9234b`.
The runtime/RPC helpers, four functions, Avro contracts and image/dependency
versions are unchanged. An initial pause attempt reached a still-starting
scheduler container; the verified prior-DAG backup was retained and the pause
was completed after container startup, before any SQL deployment.

All 503 credential-free regression tests, Ruff, 74 Python and five Bash syntax
checks passed; 26 focused tests and changed-file lint passed after the final pin.
Both Terraform roots have no drift, retaining 122 managed resources. Six labelled
test datasets/models were removed. Final DAG checkpoint is unpaused/idle,
legacy Beam remains disabled and the maintenance VM is restored stopped.
Held changed source, previous DAG/procedure, report definition and complete
native/production receipts are under
`gs://raw-historic-data/operations/2026-10-29/bounded-fill/`; the ignored parent
checklist records the completion manifest generation and SHA256.

Positive publication was isolated. This is not a current successful production
release, attained service level, forecast-accuracy result or rendered external
BI report acceptance. Revision precedence, product failure presentation,
complete lineage manifests and model evaluation remain their numbered items.
