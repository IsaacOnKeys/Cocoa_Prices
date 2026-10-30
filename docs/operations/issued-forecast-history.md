# Issued forecasts and observation vintages

Checklist item 21, branch `codex/issued-forecast-history`.

Atomic releases already retain forecasts, historical scores, frozen inputs,
models and release metadata by release ID. This feature exposes that existing
history through three additive views rather than creating another copy or
changing the daily pipeline:

| View in `cocoa_related_studio` | Meaning |
| --- | --- |
| `issued_forecasts` | Forecast values actually published in an accepted release, with original issue time, feature/input cutoff, target date, calendar days from issue to target, model, transformation SHA256, release ID and units. |
| `retrospective_scores` | Historical scores recomputed by a release's model, explicitly labeled retrospective and not chronological out-of-sample evaluation. |
| `source_observation_vintages` | The baseline and incoming source rows frozen for each accepted release, including acquisition/observation identities, producer/receipt times and incoming missing revisions. |

Later releases and rollbacks do not change an earlier issue's stored values or
issue time. The views read retained artifacts joined to published release
records; they never join the current release marker or current serving values.
Failed/unpublished candidates are excluded. Legacy snapshots with no verified
publication time are not assigned a fabricated issue time.

`horizon_days_from_issue` is target date minus the original publication's
Berlin date, including late releases. It is not a claim that the existing
single-40-day model was trained for every displayed horizon. The current
model's feature-origin/target labeling and the owner's choice of forecast
product are addressed in item 22. The input cutoff is the complete release's
snapshot cutoff, not a claim that its revised source history was available at
each historical feature origin.

`source_observation_vintages.kind` distinguishes the inherited baseline from
the current acquisition. Null acquisition provenance in older baseline rows
remains unknown. Incoming missing revisions remain visible even when the
release preserved a prior valid value. These are project-acquisition vintages;
they do not reconstruct unavailable publisher revision history before this
project archived it. Join a vintage to the same release ID when reproducing
scores; do not silently evaluate an old issue against today's revised history.

Render `forecast_history_views` with the existing SQL renderer and create the
three views. There is no new model training, stored duplicate, scheduled query,
dependency or infrastructure. Existing retained artifacts remain subject to
the release system's retention policy; this feature does not prune them.

The budget target is under EUR 5/month, with a EUR 10/month maximum including
validation. View creation uses metadata operations; consumer queries still
count toward warehouse usage. Filter by business date/release ID when exploring
history rather than repeatedly downloading all vintages.

Completed 2026-10-30: 11 focused Python 3.11 tests, changed-file Ruff and Git
whitespace checks passed. BigQuery accepted all three view definitions at
15:22:09-15:22:10 UTC. One read, capped and billed at 20 MiB, returned both
retained October 30 issues: 40 forecasts each, original release/model/cutoff
metadata present, and calendar target leads 1-40. This checks preservation and
labeling, not forecasting accuracy. Existing feature tests were not rerun.
No VM, source acquisition, training or publication run was needed.
Evidence: `.build/issued-forecast-history/deployed.json`.
