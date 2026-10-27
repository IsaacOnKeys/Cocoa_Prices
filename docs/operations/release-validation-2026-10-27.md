# Production release validation: 2026-10-27

The reconciled consumers are deployed and one controlled production cycle
passed. PR #3 was merged into master as
`553adb015b95638b065f0a2b1f2e3918cf26001a`; all three merged consumer blobs
matched the prepared candidates. Post-merge GitHub CI passed on Python 3.11
and 3.12, including 55 tests per version.

## Deployment and recovery

| Source | Installed target | Release action |
| --- | --- | --- |
| pipelines/streaming/cocoa_stream.py | /opt/beam/cocoa/local_cocoa_stream.py | Updated |
| pipelines/streaming/weather_stream.py | /opt/beam/weather/local_weather_stream.py | Updated |
| pipelines/streaming/oil_stream.py | /opt/beam/oil/local_oil_stream.py | Kept installed version; source differs only in line endings |

Preflight checked source hashes, ownership, permissions, unit mappings,
allowlisted process configuration, schemas, DAG source and absence of active
DAG runs. All candidates compiled with the VM interpreter. Effective routes
and populated/nullable Avro row transformations matched installed behavior.

The two updated files retained UID 999, GID 993 and mode 0644. All three
consumers remained active with zero restarts after validation; oil's process
was not restarted by deployment. Airflow database, scheduler and triggerer
health checks passed.

Exact source backups and a deployment receipt are retained privately at
`/opt/beam/releases/2026-10-27-553adb0/` on the VM. The ignored local
`.build/release-prep-2026-10-27/` contains checksummed candidate/backup files,
a verified recovery archive, merge proof and deployment/recovery instructions.
Ignored `.build/release-2026-10-27/` holds execution and query evidence.
Source recovery does not undo acknowledged messages or BigQuery writes.

## Production cycle

Run ID: `release_validation__553adb0__20261027`.
The existing daily DAG ran from 11:11:59 to 11:13:38 Europe/Berlin.
All ten tasks succeeded, including all three producer triggers, all three
fresh-row sensors and the BigQuery procedure task. No sensor was skipped.

| Feed | Staging rows before | After | Latest observation | Latest ingestion (UTC) |
| --- | ---: | ---: | --- | --- |
| Cocoa | 699 | 701 | 2026-10-27 | 2026-10-27 10:12:20 |
| Oil | 50 | 51 | 2026-10-20 | 2026-10-27 10:12:32 |
| Weather | 425 | 433 | 2026-10-27 | 2026-10-27 10:12:39 |

Fresh ingestion is distinct from observation freshness: the oil source
observation remained October 20. Historical windows can also republish dates;
staging row counts are not counts of unique observations.

Joined data, engineered features and historical predictions now contain 4,682
daily rows through October 27. The forecast table contains exactly 40 unique,
finite, positive values dated October 28 through December 6.

Thirteen read-only BigQuery checks returned zero violations: price lags and
40-day labels; rolling means; calendar categories; joined duplicate dates,
calendar gaps and weather sentinels; forecast count, duplicates, invalid values
and date alignment; historical horizon and actual-value alignment; and finite
evaluation metric output.

The dashboard KPI view returned one complete row dated October 27. Both indexed
series views returned 4,682 rows through October 27 with no null outputs.
The freshness view reflected the new ingestion timestamp for every feed.

## SQL source reconciliation

The deployed procedure predated this release and differed from the original
repository copy. Its calendar starts on 2014-01-02 and extends through
CURRENT_DATE(); weather values of -999.0 become null before forward fill.
The repository procedure and stage documentation now reflect this existing
behavior. Executable tokens match the captured deployed definition after
comments, whitespace and the final delimiter are normalized.

The BigQuery procedure was not redeployed. The controlled DAG run refreshed
data, trained the model and generated outputs using that existing procedure.

## Boundaries

The VM was restored to its original stopped state outside its scheduled window;
the normal daily start/stop jobs and weekday DAG schedule remain unchanged.
No schema, subscription, table definition, dependency, systemd unit, secret or
Terraform change was deployed.

This is evidence of one successful production cycle. It does not establish
ongoing reliability, chronological out-of-sample forecasting accuracy or
working dead-letter routing. BigQuery dashboard sources were checked; rendered
Looker charts and browser refresh/filter behavior were not visually verified.
