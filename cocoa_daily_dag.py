import time
from datetime import datetime, timezone

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.python import PythonSensor

TZ = pendulum.timezone("Europe/Berlin")

GCP_PROJECT = Variable.get("GCP_PROJECT", default_var="cocoa-prices-430315")
BQ_LOC = Variable.get("BQ_LOCATION", default_var="europe-west3")
BQ_PROC = Variable.get("BQ_PROC", default_var="cocoa_related.run_daily_pipeline")
BQ_PROC_FQN = f"{GCP_PROJECT}.{BQ_PROC}"


BQ_STG = {
    "cocoa": Variable.get(
        "BQ_STAGING_COCOA", default_var=f"{GCP_PROJECT}.stream_staging.cocoa_prices"
    ),
    "oil": Variable.get(
        "BQ_STAGING_OIL", default_var=f"{GCP_PROJECT}.stream_staging.oil_prices"
    ),
    "weather": Variable.get(
        "BQ_STAGING_WEATHER",
        default_var=f"{GCP_PROJECT}.stream_staging.precipitation_moisture",
    ),
}

# progress tuning
PROGRESS_TIMEOUT_MIN = int(
    Variable.get("PROGRESS_TIMEOUT_MIN", default_var="10")
)  # per-feed timeout
PROGRESS_POLL_SEC = int(Variable.get("PROGRESS_POLL_SEC", default_var="10"))
PROGRESS_REQUIRE_ALL = (
    Variable.get("PROGRESS_REQUIRE_ALL", default_var="false").lower() == "true"
)
PROGRESS_QUIET_SEC = int(Variable.get("PROGRESS_QUIET_SEC", default_var="90"))
PROGRESS_MIN_NEW_ROWS = int(
    Variable.get("PROGRESS_MIN_NEW_ROWS", default_var="1")
)  # threshold per feed

# function URLs
COCOA_FN_URL = Variable.get("COCOA_FN_URL", default_var=None)
OIL_FN_URL = Variable.get("OIL_FN_URL", default_var=None)
WEATHER_FN_URL = Variable.get("WEATHER_FN_URL", default_var=None)

FEEDS = [
    {"key": "cocoa", "table": BQ_STG["cocoa"], "url": COCOA_FN_URL},
    {"key": "oil", "table": BQ_STG["oil"], "url": OIL_FN_URL},
    {"key": "weather", "table": BQ_STG["weather"], "url": WEATHER_FN_URL},
]


def _bq_scalar(sql: str):
    hook = BigQueryHook(
        gcp_conn_id="gcp_default", location=BQ_LOC, use_legacy_sql=False
    )
    rows = hook.get_records(sql)
    return rows[0][0] if rows and rows[0] else None


def _bq_max_ts(table_fqn: str):
    return _bq_scalar(f"SELECT MAX(ingestion_time) FROM `{table_fqn}`")


def _bq_count_since(table_fqn: str, baseline_ts):
    if baseline_ts is None:
        return _bq_scalar(f"SELECT COUNT(*) FROM `{table_fqn}`") or 0
    # normalize to 'YYYY-MM-DD HH:MM:SS.ffffff UTC'
    if getattr(baseline_ts, "tzinfo", None) is None:
        baseline_ts = baseline_ts.replace(tzinfo=timezone.utc)
    else:
        baseline_ts = baseline_ts.astimezone(timezone.utc)
    ts_str = baseline_ts.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
    return (
        _bq_scalar(
            f"""
        SELECT COUNT(*)
        FROM `{table_fqn}`
        WHERE ingestion_time > TIMESTAMP('{ts_str}')
        """
        )
        or 0
    )


def capture_baseline_fn(ti):
    ti.xcom_push(
        key="baseline",
        value={f["key"]: _bq_max_ts(f["table"]) for f in FEEDS},
    )


def _invoke_or_skip(feed_key: str, url: str | None):
    """
    - If URL missing/404/unhealthy => skip this feed (softly).
    - Otherwise POST with an ID token (like your original).
    """
    if not url:
        raise AirflowSkipException(f"{feed_key}: no URL configured — skipping.")

    import requests as rq
    from google.auth.transport.requests import Request
    from google.oauth2 import id_token

    try:
        # Auth header for both HEAD and POST (Cloud Run often requires auth even for HEAD/GET)
        token = id_token.fetch_id_token(Request(), url)
        headers = {"Authorization": f"Bearer {token}"}

        # Fast health check first
        try:
            r = rq.head(url, headers=headers, timeout=15)
            if r.status_code == 405:  # method not allowed -> fall back to GET
                r = rq.get(url, headers=headers, timeout=15)
        except Exception:
            # Network or HEAD not supported; we continue to POST but catch 404s below
            r = None

        if r is not None and r.status_code == 404:
            raise AirflowSkipException(
                f"{feed_key}: endpoint 404 — skipping this feed."
            )

        # Invoke producer
        resp = rq.post(url, headers=headers, timeout=120)
        if resp.status_code == 404:
            raise AirflowSkipException(f"{feed_key}: endpoint 404 on POST — skipping.")
        resp.raise_for_status()
        return {"invoked": True, "status": resp.status_code}

    except AirflowSkipException:
        raise
    except Exception as e:
        # Treat transient/unexpected issues as a soft skip to allow partial runs
        raise AirflowSkipException(
            f"{feed_key}: trigger health-check/invoke failed ({e}) — skipping."
        )


def make_wait_callable(feed_key: str, table_fqn: str):
    """
    Factory for PythonSensor callables. Returns True when this feed has >= MIN_NEW_ROWS beyond its baseline.
    """

    def _wait_callable(**ctx):
        ti = ctx["ti"]
        baseline = (
            ti.xcom_pull(task_ids="capture_baseline", key="baseline") or {}
        ).get(feed_key)
        cnt = _bq_count_since(table_fqn, baseline)
        ctx["task_instance"].log.info(
            "Feed %s: baseline=%s, new_rows=%s (threshold=%s)",
            feed_key,
            baseline,
            cnt,
            PROGRESS_MIN_NEW_ROWS,
        )
        return cnt >= PROGRESS_MIN_NEW_ROWS

    return _wait_callable


with DAG(
    dag_id="cocoa_daily",
    schedule="0 19 * * *",
    start_date=datetime(2025, 9, 9, tzinfo=TZ),
    catchup=False,
    max_active_runs=1,
    tags=["cocoa"],
) as dag:

    # 0) Baseline watermarks (once)
    capture_baseline = PythonOperator(
        task_id="capture_baseline",
        python_callable=capture_baseline_fn,
    )

    # 1) Triggers per feed (skip if unhealthy / 404)
    trigger_tasks = []
    for f in FEEDS:
        trigger = PythonOperator(
            task_id=f"trigger_{f['key']}",
            python_callable=lambda f=f: _invoke_or_skip(f["key"], f["url"]),
        )
        capture_baseline >> trigger
        trigger_tasks.append(trigger)

    # 2) Wait per feed (soft-fail => SKIPPED on timeout)
    wait_tasks = []
    for f in FEEDS:
        wait = PythonSensor(
            task_id=f"wait_{f['key']}_rows",
            python_callable=make_wait_callable(f["key"], f["table"]),
            poke_interval=PROGRESS_POLL_SEC,
            timeout=PROGRESS_TIMEOUT_MIN * 60,
            mode="reschedule",
            soft_fail=True,  # timeout => SKIPPED, not FAILED
        )
        # If producers run asynchronously after the trigger, wait after each trigger
        trigger_tasks[FEEDS.index(f)] >> wait
        wait_tasks.append(wait)

    # 3) Quiet period to let ingestion settle (runs if any upstream wait succeeded)
    # If you require ALL feeds to have data, set PROGRESS_REQUIRE_ALL=true
    from airflow.utils.trigger_rule import TriggerRule

    settle_trigger_rule = (
        TriggerRule.ALL_SUCCESS if PROGRESS_REQUIRE_ALL else TriggerRule.ONE_SUCCESS
    )

    sleep_after_ingest = BashOperator(
        task_id="sleep_after_ingest",
        bash_command=f"sleep {PROGRESS_QUIET_SEC}",
        trigger_rule=settle_trigger_rule,
    )

    # 4) Run the downstream BQ pipeline
    run_bq_pipeline = BigQueryInsertJobOperator(
        task_id="run_bq_pipeline",
        gcp_conn_id="gcp_default",
        configuration={
            "query": {"query": f"CALL `{BQ_PROC_FQN}`();", "useLegacySql": False}
        },
        location=BQ_LOC,
        retries=2,
        trigger_rule=settle_trigger_rule,
    )

    # 5) Optional: summary of branch results (always runs at the end)
    def summarize(**ctx):
        dr = ctx["ti"].get_dagrun()
        states = {}
        for f in FEEDS:
            ti = dr.get_task_instance(f"wait_{f['key']}_rows")
            states[f["key"]] = ti.state if ti else "unknown"
        print("Feed states:", states)
        return states

    summary = PythonOperator(
        task_id="summary",
        python_callable=summarize,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Wiring
    # capture_baseline -> triggers -> waits -> sleep -> pipeline -> summary
    capture_baseline >> trigger_tasks
    chain(wait_tasks, sleep_after_ingest, run_bq_pipeline, summary)
