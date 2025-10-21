# $AIRFLOW_HOME/dags/cocoa_daily_dag.py
from datetime import datetime, timedelta, timezone
from typing import Union

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sensors.python import PythonSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.trigger_rule import TriggerRule

# ===== Config =====
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

# Progress tuning (overridable via Variables)
PROGRESS_TIMEOUT_MIN = int(Variable.get("PROGRESS_TIMEOUT_MIN", default_var="30"))
PROGRESS_POLL_SEC = int(Variable.get("PROGRESS_POLL_SEC", default_var="20"))
PROGRESS_REQUIRE_ALL = (
    Variable.get("PROGRESS_REQUIRE_ALL", default_var="false").lower() == "true"
)
PROGRESS_QUIET_SEC = int(Variable.get("PROGRESS_QUIET_SEC", default_var="120"))
PROGRESS_MIN_NEW_ROWS = int(Variable.get("PROGRESS_MIN_NEW_ROWS", default_var="1"))

# Producer endpoints (missing URL => soft-skip)
COCOA_FN_URL = Variable.get("COCOA_FN_URL", default_var=None)
OIL_FN_URL = Variable.get("OIL_FN_URL", default_var=None)
WEATHER_FN_URL = Variable.get("WEATHER_FN_URL", default_var=None)

# SSH connection (Airflow Connection ID)
SSH_CONN_ID = Variable.get("SSH_CONN_ID", default_var="ssh_vm")

# Feed registry (with host systemd service names)
FEEDS = [
    {
        "key": "cocoa",
        "table": BQ_STG["cocoa"],
        "url": COCOA_FN_URL,
        "service": "beam-cocoa",
    },
    {"key": "oil", "table": BQ_STG["oil"], "url": OIL_FN_URL, "service": "beam-oil"},
    {
        "key": "weather",
        "table": BQ_STG["weather"],
        "url": WEATHER_FN_URL,
        "service": "beam-weather",
    },
]


# ===== Helpers =====
def _bq_scalar(sql: str):
    hook = BigQueryHook(
        gcp_conn_id="gcp_default", location=BQ_LOC, use_legacy_sql=False
    )
    rows = hook.get_records(sql)
    return rows[0][0] if rows and rows[0] else None


def _bq_max_ts(table_fqn: str):
    return _bq_scalar(f"SELECT MAX(ingestion_time) FROM `{table_fqn}`")


def _to_utc_datetime(ts: Union[datetime, float, int, str]):
    if ts is None:
        return None
    if isinstance(ts, (int, float)):  # epoch seconds
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if isinstance(ts, str):  # ISO-ish or "... UTC"
        s = ts
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            dt = datetime.fromisoformat(s.replace(" UTC", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    if isinstance(ts, datetime):  # naive/aware datetime
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    if hasattr(ts, "isoformat"):  # pendulum DateTime
        return _to_utc_datetime(ts.isoformat())
    raise TypeError(f"Unsupported baseline type: {type(ts)}")


def _bq_count_since(table_fqn: str, baseline_ts):
    baseline_dt = _to_utc_datetime(baseline_ts)
    if baseline_dt is None:
        return _bq_scalar(f"SELECT COUNT(*) FROM `{table_fqn}`") or 0
    ts_str = baseline_dt.strftime("%Y-%m-%d %H:%M:%S.%f UTC")
    return (
        _bq_scalar(
            f"""
            SELECT COUNT(*) FROM `{table_fqn}`
            WHERE ingestion_time >= TIMESTAMP('{ts_str}')
            """
        )
        or 0
    )


def capture_baseline_fn(ti):
    ti.xcom_push(
        key="baseline", value={f["key"]: _bq_max_ts(f["table"]) for f in FEEDS}
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

        # best-effort probe (HEAD only). If 404, skip; otherwise proceed.
        try:
            r = rq.head(url, headers=headers, timeout=15)
            if r.status_code == 404:
                raise AirflowSkipException(f"{feed_key}: endpoint 404 — skipping.")
        except Exception:
            pass

        # single invocation (POST only)
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
        baseline = (
            ti.xcom_pull(task_ids="capture_baseline", key="baseline") or {}
        ).get(feed_key)
        cnt = _bq_count_since(table_fqn, baseline)
        ti.log.info(
            "Feed %s: baseline=%s, new_rows=%s (threshold=%s)",
            feed_key,
            baseline,
            cnt,
            PROGRESS_MIN_NEW_ROWS,
        )
        return cnt >= PROGRESS_MIN_NEW_ROWS

    return _wait_callable


# ===== DAG =====
with DAG(
    dag_id="cocoa_daily",
    schedule="0 19 * * 1-5",  # 19:00 Mon–Fri (Europe/Berlin)
    start_date=pendulum.datetime(2025, 9, 9, tz=TZ),
    catchup=False,
    max_active_runs=1,
    tags=["cocoa"],
) as dag:

    capture_baseline = PythonOperator(
        task_id="capture_baseline",
        python_callable=capture_baseline_fn,
    )

    trigger_tasks, wait_tasks, stop_tasks = [], [], []

    for f in FEEDS:
        # 1) Trigger the producer (HTTP). Missing URL => SKIPPED.
        trigger = PythonOperator(
            task_id=f"trigger_{f['key']}",
            python_callable=lambda f=f: _invoke_or_skip(f["key"], f["url"]),
        )

        # 2) Start ONLY this feed's Beam service on the VM (via SSH) if trigger succeeded.
        start_beam = SSHOperator(
            task_id=f"start_beam_{f['key']}",
            ssh_conn_id=SSH_CONN_ID,
            command=f"sudo systemctl start {f['service']}",
        )

        # 3) Wait for at least N new rows since baseline.
        wait_rows = PythonSensor(
            task_id=f"wait_{f['key']}_rows",
            python_callable=make_wait_callable(f["key"], f["table"]),
            poke_interval=PROGRESS_POLL_SEC,
            timeout=PROGRESS_TIMEOUT_MIN * 60,
            mode="reschedule",
            soft_fail=True,  # timeout => SKIPPED
        )

        # 4) Stop this Beam service regardless of branch outcome.
        stop_beam = SSHOperator(
            task_id=f"stop_beam_{f['key']}",
            ssh_conn_id=SSH_CONN_ID,
            command=f"sudo systemctl stop {f['service']}",
            trigger_rule=TriggerRule.ALL_DONE,
        )

        # Wire the branch: baseline → trigger → start → wait → stop
        capture_baseline >> trigger >> start_beam >> wait_rows >> stop_beam

        trigger_tasks.append(trigger)
        wait_tasks.append(wait_rows)
        stop_tasks.append(stop_beam)

    # Proceed if at least one feed produced rows (configurable)
    settle_trigger_rule = (
        TriggerRule.ALL_SUCCESS if PROGRESS_REQUIRE_ALL else TriggerRule.ONE_SUCCESS
    )

    # Quiet period to let late messages flush through writers
    sleep_after_ingest = TimeDeltaSensor(
        task_id="sleep_after_ingest",
        delta=timedelta(seconds=PROGRESS_QUIET_SEC),
        mode="reschedule",
    )

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

    # Final safety stop (cheap & idempotent)
    stop_all_streams = SSHOperator(
        task_id="stop_beam_all",
        ssh_conn_id=SSH_CONN_ID,
        command="sudo systemctl stop beam-cocoa beam-oil beam-weather",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(wait_tasks, sleep_after_ingest, run_bq_pipeline, summary, stop_all_streams)
