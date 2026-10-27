# Historical Beam backfills

These pipelines process the one-time historical bootstrap snapshots into BigQuery
curated history. Automated daily ingestion is implemented separately by
[functions](../../functions/), [streaming consumers](../streaming/) and
[Airflow](../../airflow/dags/cocoa_daily.py).

| Script | Protected snapshot input | Existing curated output |
| --- | --- | --- |
| clean_cocoa_prices.py | snapshots/2025-10-02/icco/Daily_Prices_Home.csv | cocoa_related.cocoa |
| clean_oil_fred.py | snapshots/2025-10-02/fred/brent_oil_fred.json | cocoa_related.brent_prices |
| clean_precipitation.py | snapshots/2025-10-02/nasa_power/POWER_Point_Daily.csv | cocoa_related.precipitation |

All inputs are in gs://raw-historic-data/. Exact generations/checksums are
recorded in [the historical manifest](../../data/historical_manifest.yaml).
Object holds protect the names used by Beam against replacement.

## Source environment and historical runners

Keep the Beam environment separate:

~~~bash
python -m pip install -r pipelines/requirements.txt
~~~

The existing scripts retain DirectRunner and historical DataflowRunner support,
destinations, validation and WRITE_TRUNCATE behavior. Moving paths does not
make a production backfill safe to run as a check. No backfill was executed
during repository validation.

Their REQUIREMENTS_FILE references resolve to pipelines/requirements.txt
from the script location. That existing variable remains unused by the actual
PipelineOptions; this migration did not activate previously unused options.
For a separately reviewed Dataflow submission, pass an explicit requirements
file using the supported Beam CLI options. Historical custom-image references
in source comments document earlier usage, not an existing deployment pipeline.

## Backfill Docker recipe versus Airflow Docker

~~~bash
docker build -f pipelines/backfill/Dockerfile pipelines/backfill
~~~

The recipe preserves Python 3.12-slim, Beam 2.61.0, /app paths and the Python
entry point. It copies the three cleaners using this directory as build context.
Its dependency version is intentionally preserved and differs from the current
VM streaming environment (Beam 2.65.0). Reconciliation is separate engineering
work; the image is not advertised as interchangeable with that environment.

The recipe built successfully in a disposable VM context. No image was
published and no pipeline container was run against production.

Airflow uses the separate captured
[Docker Compose definition](../../airflow/docker-compose.yaml), based on
apache/airflow:2.10.2 and postgres:13. Its live /opt/airflow paths are independent
of this repository layout. No current cocoa-code-project Artifact Registry
repository was found; gcf-artifacts is owned by Gen2 function builds.
