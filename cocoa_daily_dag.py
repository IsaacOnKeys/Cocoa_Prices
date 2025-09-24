# $AIRFLOW_HOME/dags/cocoa_daily.py
from datetime import timezone

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
from airflow.utils.trigger_rule import TriggerRule

# ----- Config -----
TZ = pendulum.timezone("Europe/Berlin")

GCP_PROJECT = Variable.get("GCP_PROJECT", default_var="cocoa-prices-430315")
BQ_LOC = Variable.get("BQ_LOCATION", default_var="europe-west3")
BQ_PROC = Variable.get("BQ_PROC", default_var="cocoa_related.run_daily_pipeline")
BQ_PROC_FQN = f"{GCP_PROJECT}.{BQ_PROC}"

BQ_STG = {
    "cocoa": Variable.get("BQ_STAGING_COCOA", default_var=f"{GCP_PROJECT}.stream_staging.cocoa_prices"),
    "oil": Variable.get("BQ_STAGING_OIL", default_var=f"{GCP_PROJECT}.stream_staging.oil_prices"),
    "weather": Variable.get("BQ_STAGING_WEATHER", default_var=f"{GCP_PROJECT}.stream_staging.precipitation_moisture"),
}

# Progress tuning
PROGRESS_TIMEOUT_MIN = int(Variable.get("PROGRESS_TIMEOUT_MIN", default_var="30"))
PROGRESS_POLL_SEC = int(Variable.get("PROGRESS_POLL_SEC", default_var="20"))
PROGRESS_REQUIRE_ALL = Variable.get("PROGRESS_REQUIRE_ALL", default_var="false").lower() == "true"
PROGRESS_QUIET_SEC = int(Variable.get("PROGRESS_QUIET_SEC", default_var="120"))
PROGRESS_MIN_NEW_ROWS = int(Variable.get("PROGRESS_MIN_NEW_ROWS", default_var="1"))

# Producer endpoints (HTTP/Cloud Run; missing URL => task is skipped)
COCOA_FN_URL = Variable.get("COCOA_FN_URL", default_var=None)
OIL_FN_URL = Variable.get("OIL_FN_URL", default_var=None)
WEATHER_FN_URL = Variable.get("WEATHER_FN_URL", default_var=None)

# Map feeds to their systemd service names
FEEDS = [
    {"key": "cocoa",   "table": BQ_STG["cocoa"],   "url": COCOA_FN_URL,   "service": "beam-cocoa"},
    {"key": "oil",     "table": BQ_STG["oil"],     "url": OIL_FN_URL,     "service": "beam-oil"},
    {"key": "weather", "table": BQ_STG["weather"], "url": WEATHER_FN_URL, "service": "beam-weather"},
]

# ----- Helpers -----
def _bq_scalar(sql: str):
    hook = BigQueryHook(gcp_conn_id="gcp_default", location=BQ_LOC, use_legacy_sql=False)
    rows = hook.get_records(sql)
    return rows[0][0] if rows and rows[0] else None

def _bq_max_ts(table_fqn: str):
    return _bq_scalar(f"SELECT MAX(ingestion_time) FROM `{table_fqn}`")

def _bq_count_since(table_fqn: str, baseline_ts):
    if baseline_ts is None:
        return _bq_scalar(f"SELECT COUNT(*) FROM `{table_fqn}`") or 0
    if isinstance(baseline_ts, str):
        baseline_ts = pendulum.parse(baseline_ts)
    if getattr(baseline_ts, "tzinfo", None) is None:
        baseline_ts = baseline_ts.replace(tzinfo=timezone.utc)
    else:
        baseline_ts = baseline_ts.astimezone(timezone.utc)
    ts_str = baseline_ts.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
    return _bq_scalar(
        f"""
        SELECT COUNT(*) FROM `{table_fqn}`
        WHERE ingestion_time >= TIMESTAMP('{ts_str}')
        """
    ) or 0

def capture_baseline_fn(ti):
    ti.xcom_push(
        key="baseline",
        value={f["key"]: _bq_max_ts(f["table"]) for f in FEEDS},
    )

def _invoke_or_skip(feed_key: str, url: str | None):
    if not url:
        raise AirflowSkipException(f"{feed_key}: no URL configured — skipping.")

    import requests as rq
    from google.auth.transport.requests import Request
    from google.oauth2 import id_token

    try:
        token = id_token.fetch_id_token(Request(), url)
        headers = {"Authorization": f"Bearer {token}"}

        try:
            r = rq.head(url, headers=headers, timeout=15)
            if r.status_code == 405:
                r = rq.get(url, headers=headers, timeout=15)
            if r.status_code == 404:
                raise AirflowSkipException(f"{feed_key}: endpoint 404 — skipping.")
        except Exception:
            pass

        resp = rq.post(url, headers=headers, timeout=120)
        if resp.status_code == 404:
            raise AirflowSkipException(f"{feed_key}: endpoint 404 on POST — skipping.")
        resp.raise_for_status()
        return {"invoked": True, "status": resp.status_code}
    except AirflowSkipException:
        raise
    except Exception as e:
        raise AirflowSkipException(f"{feed_key}: invoke failed ({e}) — skipping.")

def make_wait_callable(feed_key: str, table_fqn: str):
    def _wait_callable(**ctx):
        ti = ctx["ti"]
        baseline = (ti.xcom_pull(task_ids="capture_baseline", key="baseline") or {}).get(feed_key)
        cnt = _bq_count_since(table_fqn, baseline)
        ti.log.info("Feed %s: baseline=%s, new_rows=%s (threshold=%s)",
                    feed_key, baseline, cnt, PROGRESS_MIN_NEW_ROWS)
        return cnt >= PROGRESS_MIN_NEW_ROWS
    return _wait_callable

# ----- DAG -----
with DAG(
    dag_id="cocoa_daily",
    schedule="0 19 * * 1-5",              # 19:00 Mon–Fri (Europe/Berlin)
    start_date=pendulum.datetime(2025, 9, 9, tz=TZ),
    catchup=False,
    max_active_runs=1,
    tags=["cocoa"],
) as dag:

    # Baseline timestamp per feed (to measure new rows)
    capture_baseline = PythonOperator(
        task_id="capture_baseline",
        python_callable=capture_baseline_fn,
    )

    trigger_tasks, wait_tasks, stop_tasks = [], [], []

    for f in FEEDS:
        # 1) Trigger the producer (HTTP). If URL is missing/404, task is SKIPPED.
        trigger = PythonOperator(
            task_id=f"trigger_{f['key']}",
            python_callable=lambda f=f: _invoke_or_skip(f["key"], f["url"]),
        )

        # 2) Start ONLY the corresponding Beam service if trigger succeeded.
        start_beam = BashOperator(
            task_id=f"start_beam_{f['key']}",
            bash_command=f"sudo systemctl start {f['service']}",
            # default trigger_rule=ALL_SUCCESS makes this naturally skip if trigger was skipped/failed
        )

        # 3) Wait for at-least-N new rows since baseline (releases worker slots between pokes).
        wait_rows = PythonSensor(
            task_id=f"wait_{f['key']}_rows",
            python_callable=make_wait_callable(f["key"], f["table"]),
            poke_interval=PROGRESS_POLL_SEC,
            timeout=PROGRESS_TIMEOUT_MIN * 60,
            mode="reschedule",          # free the worker between checks
            soft_fail=True,             # timeout -> SKIPPED instead of FAILED
        )

        # 4) Stop that Beam service regardless of wait outcome (idempotent if already stopped).
        stop_beam = BashOperator(
            task_id=f"stop_beam_{f['key']}",
            bash_command=f"sudo systemctl stop {f['service']}",
            trigger_rule=TriggerRule.ALL_DONE,   # stop even if upstream skipped/failed
        )

        # Wire the branch: baseline -> trigger -> start -> wait -> stop
        capture_baseline >> trigger >> start_beam >> wait_rows >> stop_beam

        trigger_tasks.append(trigger)
        wait_tasks.append(wait_rows)
        stop_tasks.append(stop_beam)

    # If at least one feed delivered rows, proceed; otherwise skip downstream.
    settle_trigger_rule = TriggerRule.ALL_SUCCESS if PROGRESS_REQUIRE_ALL else TriggerRule.ONE_SUCCESS

    sleep_after_ingest = BashOperator(
        task_id="sleep_after_ingest",
        bash_command=f"sleep {PROGRESS_QUIET_SEC}",
        trigger_rule=settle_trigger_rule,
    )

    run_bq_pipeline = BigQueryInsertJobOperator(
        task_id="run_bq_pipeline",
        gcp_conn_id="gcp_default",
        configuration={"query": {"query": f"CALL `{BQ_PROC_FQN}`();", "useLegacySql": False}},
        location=BQ_LOC,
        retries=2,
        trigger_rule=settle_trigger_rule,
    )

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

    # Optional final safety stop (cheap & idempotent) to ensure all are down at DAG end
    stop_all_streams = BashOperator(
        task_id="stop_beam_all",
        bash_command="sudo systemctl stop beam-cocoa beam-oil beam-weather",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Central path waits on all the per-feed waits (with ONE_SUCCESS/ALL_SUCCESS), then runs BQ, then summary.
    chain(wait_tasks, sleep_after_ingest, run_bq_pipeline, summary, stop_all_streams)
