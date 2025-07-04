from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.functions import (
    CloudFunctionInvokeFunctionOperator,
)
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator

default_args = {
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="trigger_daily_functions",
    schedule_interval="0 19 * * 1-5",  # 19:00 Mon-Fri
    catchup=False,
    default_args=default_args,
    tags=["cocoa"],
) as dag:

    trigger_cocoa = CloudFunctionInvokeFunctionOperator(
        task_id="call_cocoa_http_fn",
        project_id="{{ var.value.project_id }}",
        location="europe-west3",
        function_id="publish_price",
        input_data=r"{}",
    )

    trigger_oil = PubSubPublishMessageOperator(
        task_id="pub_oil_message",
        project_id="{{ var.value.project_id }}",
        topic="oil-prices-topic",
        messages=[{"data": b'{"publish_oil_price":"composer"}'}],
    )

    trigger_weather = PubSubPublishMessageOperator(
        task_id="pub_weather_message",
        project_id="{{ var.value.project_id }}",
        topic="weather-topic",
        messages=[{"data": b'{"publish_weather_data":"composer"}'}],
    )

    trigger_cocoa >> [trigger_oil, trigger_weather]
