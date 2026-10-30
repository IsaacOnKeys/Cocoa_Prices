# Historical Beam backfills

These pipelines process the one-time historical bootstrap snapshots into BigQuery
curated history. Automated daily ingestion is implemented separately by
[producer functions](../../functions/), [bounded Cloud Run consumers](../../functions/ingestion/README.md) and
[Airflow](../../airflow/dags/cocoa_daily.py).

| Script | Protected snapshot input | Existing curated output |
| --- | --- | --- |
| clean_cocoa_prices.py | snapshots/2025-10-02/icco/Daily_Prices_Home.csv | cocoa_related.cocoa |
| clean_oil_eia.py | snapshots/2026-10-27/eia/brent_oil_eia.json | cocoa_related.brent_prices |
| clean_precipitation.py | snapshots/2025-10-02/nasa_power/POWER_Point_Daily.csv | cocoa_related.precipitation |

All inputs are in gs://raw-historic-data/. Exact generations/checksums are
recorded in [the historical manifest](../../data/historical_manifest.yaml).
Object holds protect the names used by Beam against replacement.

Oil history is acquired directly from EIA's dated published table. Its API is
used for daily acquisition only when the requested interval agrees with that
table. The accepted archive uses response.data with period/value/series/units;
the cleaner rejects API-only archives and incompatible products/units. See the
[migration evidence](../../docs/operations/eia-migration.md). No historical
warehouse replacement has been performed by changing these inputs.

## Source environment and historical runners

Keep the Beam environment separate:

~~~bash
python -m pip install -r pipelines/requirements.txt
~~~

The existing scripts retain DirectRunner and historical DataflowRunner support,
destinations and validation. Curated writes still use WRITE_TRUNCATE; invalid
tables now append JSON evidence in Errors with original input, source reference,
checksum and rejection time. NASA protocol metadata is explicitly recognized;
malformed/missing observations reach the invalid sink. Moving paths does not
make a production backfill safe to run as a check. The
[quarantine validation](../../docs/operations/invalid-record-quarantine.md)
executes actual graphs against isolated fixtures/tables and records the boundary
between DirectRunner cloud IO, worker packaging and remote Dataflow execution.

All three cleaners collapse identical facts to one valid row and retain repeats
as duplicate evidence. Conflicting facts for a date all enter the invalid sink;
group iteration order cannot select a price or weather revision. The shared
worker distribution is `cocoa-backfill-recovery` 1.1.0. Actual local/cloud graphs
and offline worker installation are recorded in
[revision-policy acceptance](../../docs/operations/deterministic-source-revisions.md).

Their REQUIREMENTS_FILE references resolve to pipelines/requirements.txt
from the script location. That existing variable remains unused by the actual
PipelineOptions; this migration did not activate previously unused options.
For a separately reviewed Dataflow submission, pass an explicit requirements
file using the supported Beam CLI options. The scripts default --setup_file to
the adjacent setup.py, staging the shared backfill_recovery module. An operator's
explicit setup file is preserved and must include that module. See [Beam's worker
dependency guidance](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/).
Historical custom-image references
in source comments document earlier usage, not an existing deployment pipeline.

## Backfill Docker recipe versus Airflow Docker

~~~bash
docker build -f pipelines/backfill/Dockerfile -t cocoa-backfill:python3.11 pipelines
~~~

The image pins its Python 3.11 base digest and installs pipelines/requirements.txt,
including Beam 2.65.0. This aligns the image with the existing nine pinned Beam
requirements instead of retaining a separate 2.61.0 dependency family. The build
context is pipelines/; its .dockerignore permits the cleaners, recovery helper,
worker setup package and runtime build inputs. Scripts remain under /app, and their /requirements.txt
submission reference exists in the image. The image retains its Python entry point.

This is a local DirectRunner/client image, not a Dataflow SDK worker harness.
For a separately reviewed Dataflow submission, use Python 3.11 with Beam 2.65.0
and matching dependencies; let Beam choose the matching managed SDK image.
A custom Dataflow worker must match both Python and Beam versions and use the
[SDK harness entry point](https://docs.cloud.google.com/dataflow/docs/guides/troubleshoot-custom-container).
The obsolete unavailable custom-image override was removed from the usage examples.
No Dataflow job is launched by the runtime validation.

See the [Python 3.11 migration](../../docs/operations/python311-runtime.md) for
actual image identities, dependency checks, real-runner fixture results and
isolated BigQuery IO validation. These tests never run the fixed WRITE_TRUNCATE
backfills against production. General safe backfill controls remain item 29.

Airflow uses the separate captured
[Docker Compose definition](../../airflow/docker-compose.yaml), based on
the built cocoa-airflow:2.10.2-python3.11 image and postgres:13. Its live /opt/airflow paths are independent
of this repository layout. No current cocoa-code-project Artifact Registry
repository was found; gcf-artifacts is owned by Gen2 function builds.
