# Cocoa Price Forecasting Pipeline

A Google Cloud data engineering project combining historical backfills,
automated daily ingestion, temporal/exogenous feature engineering, and a
40-day-ahead BigQuery ML cocoa price forecast.
The inputs are ICCO cocoa prices, FRED Brent oil observations, and NASA POWER
precipitation and soil moisture.

## Architecture

~~~mermaid
flowchart LR
    Sources["ICCO · FRED · NASA POWER"] --> Functions["Gen2 Python functions"]
    Functions --> PubSub["Pub/Sub · Avro contracts"]
    subgraph VM["Compute Engine VM"]
        Beam["Beam DirectRunner consumers"]
        subgraph Docker["Docker Compose"]
            Airflow["Airflow · LocalExecutor"]
            Postgres["PostgreSQL metadata"]
            Airflow --- Postgres
        end
    end
    Scheduler["Cloud Scheduler"] -. "VM start / stop" .-> VM
    PubSub --> Beam --> Staging["BigQuery staging"]
    Airflow -. "trigger ingestion" .-> Functions
    Airflow -. "run procedure" .-> Curated
    subgraph SQL["BigQuery procedure"]
        Curated["Curated data · MERGE + daily alignment"]
        Features["Feature engineering · lags + rolling + calendar"]
        Training["Model training · linear regression"]
        Prediction["Prediction · ML.PREDICT"]
        Evaluation["Historical predictions + MAPE"]
        Curated --> Features --> Training --> Prediction
        Prediction -->|"historical scores"| Evaluation
        Features -. "observed prices" .-> Evaluation
    end
    Staging --> Curated
    Prediction -->|"40-day forecasts"| Looker["Looker Studio"]
    Evaluation --> Looker
    Historical["One-time historical acquisition"] --> GCS["Protected GCS snapshots + manifest"]
    GCS --> Backfill["Beam historical backfills"] --> Base["BigQuery curated history"]
    Base --> Curated
~~~

Historical bootstrap and operational ingestion are separate acquisition modes.
The daily path uses functions, Pub/Sub and VM-hosted Beam. Historical inputs
were acquired once, archived with provenance, and processed by Beam backfills.
Cloud Scheduler controls VM uptime; Airflow orchestrates ingestion and SQL.

The analytical review sequence is [curated data](sql/01_curated_data/README.md)
-> [feature engineering](sql/02_feature_engineering/README.md)
-> [model training](sql/03_model_training/README.md)
-> [prediction](sql/04_prediction/README.md)
-> [backtesting / evaluation](sql/05_evaluation/README.md).
These stages have ordered directories under sql/, with inputs, outputs and
links to the implementing statements in the single executable procedure.

## Where to review the engineering

| Area | Start here | What it demonstrates |
| --- | --- | --- |
| Ingestion and contracts | [functions/](functions/), [schemas/](schemas/) | Separate producers, nullable Avro contracts, self-contained deployment artifacts |
| Streaming | [pipelines/streaming/](pipelines/streaming/) | Pub/Sub binary decoding and BigQuery staging |
| Orchestration | [airflow/dags/cocoa_daily.py](airflow/dags/cocoa_daily.py) | Feed triggers, progress sensors, quiet period and SQL coordination |
| Curated data | [sql/01_curated_data/](sql/01_curated_data/README.md) | Deduplication/MERGE, daily calendar and forward fill |
| Feature engineering | [sql/02_feature_engineering/](sql/02_feature_engineering/README.md) | 1/7/28-day cocoa lags, seven-row oil/weather means, calendar categories and the 40-day target |
| Model training | [sql/03_model_training/](sql/03_model_training/README.md) | BigQuery ML linear regression, L2 regularization, predictors/label and training-row selection |
| Prediction | [sql/04_prediction/](sql/04_prediction/README.md) | Recent 40-day-ahead forecasts and full-history scoring |
| Backtesting / evaluation | [sql/05_evaluation/](sql/05_evaluation/README.md) | Actual-price alignment, current MAPE and chronological evaluation limitations |
| Historical bootstrap | [backfills](pipelines/backfill/README.md), [data provenance](data/README.md) | Source-format handling, validation, protected historical snapshots and recovery evidence |
| Infrastructure | [infra/terraform/](infra/terraform/), [GCP ownership map](docs/infrastructure/gcp-inventory.md) | Existing infrastructure represented in state; ownership boundaries and verified no-change plans |
| Validation | [tests/unit/](tests/unit/) | Contract round trips, deterministic packaging and path compatibility without production execution |

See [architecture and design decisions](docs/architecture/README.md) for the
runtime boundaries and editable diagrams.

## Run the local checks

Use a dedicated Python 3.12 development environment; production function runtime
is Python 3.11. Runtime dependencies remain separate for functions, Beam and Airflow.

~~~bash
python -m pip install -r requirements-dev.txt
ruff check .
python scripts/development/check_syntax.py
python scripts/deploy/package_functions.py all
python -m pytest -q --basetemp=.build/pytest
~~~

These checks need no GCP credentials. The packager creates complete runtime
roots and deterministic ZIPs under ignored .build/functions/. Each contains
main.py, its existing requirements file, and a byte-identical canonical schema.

For local function execution or a future deployment, use the prepared root,
not the raw `functions/<feed>/` source directory. For example, install the cocoa
function's requirements in a separate environment and run functions-framework
with target publish_price from .build/functions/cocoa/.

## Verified evidence

Repository checks cover Avro round trips, archive membership and determinism,
source-byte preservation, helper paths, syntax and references. Additional
26 October 2026 checks verified both Terraform roots with no-change plans before
and after relocation, built the historical Docker recipe, recovered/checksummed
the archived raw snapshots, and compared the DAG task graph in an isolated
Airflow 2.10.2 image with Google provider 10.22.0.

The VM inspection and Docker startup outcome are recorded in the
[runtime handoff](docs/operations/portfolio-handoff-2026-10-26.md).
The [CI workflow](.github/workflows/ci.yml) is configured to run credential-free
checks on Python 3.11 and 3.12 after publication; see the
[CI reference](docs/operations/ci.md) for test boundaries and commands.
No forecasting accuracy, chronological out-of-sample evaluation, working
dead-letter routing, or end-to-end production equivalence is claimed from these
checks. The current historical-prediction MAPE has
[documented evaluation limitations](sql/README.md).

## Operational reference

- [Historical snapshot manifest](data/historical_manifest.yaml): exact generations,
  measured coverage, acquisition provenance, SHA-256 and recovery verification.
- [Runtime/install mappings](docs/operations/runtime-layout.md): VM paths, captured
  systemd units, sanitized Compose configuration and scheduling responsibilities.
- [Terraform workflow](infra/terraform/README.md): two separate roots and preserved
  backend identities; application deployment remains a separate responsibility.
- [Engineering backlog](docs/operations/engineering-backlog.md): remaining reliability,
  event-date semantics, evaluation, monitoring and deployment work.

Full historical files stay in private GCS and ignored local caches. Samples and
local validation remain available without cloud access. Production ingestion,
backfills and installer snippets perform real external work and are intentionally
outside the local check commands.
