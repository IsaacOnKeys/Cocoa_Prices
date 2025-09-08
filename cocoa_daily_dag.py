from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

TZ = pendulum.timezone("Europe/Berlin")

with DAG(
    dag_id="cocoa_daily",
    schedule="0 19 * * *",  # 19:00 Europe/Berlin daily
    start_date=datetime(2025, 8, 1, tzinfo=TZ),
    catchup=False,  # per your preference
    default_view="graph",
    tags=["cocoa"],
    max_active_runs=1,
) as dag:

    # Task A: health-check (placeholder for now)
    health_check = EmptyOperator(task_id="health_check")

    # Task B: trigger ingestion (placeholder for now)
    trigger_ingestion = EmptyOperator(task_id="trigger_ingestion")

    # Task C: run BigQuery pipeline
    run_bq_pipeline = BigQueryInsertJobOperator(
        task_id="run_bq_pipeline",
        configuration={
            "query": {
                "query": "CALL `cocoa-prices-430315.cocoa_related.run_daily_pipeline`()",
                "useLegacySql": False,
            }
        },
        location="europe-west3",
        retries=2,
    )

    health_check >> trigger_ingestion >> run_bq_pipeline
