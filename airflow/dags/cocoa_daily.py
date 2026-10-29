# $AIRFLOW_HOME/dags/cocoa_daily_dag.py
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import re
import time
import uuid

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule

# ===== Config =====
TZ = pendulum.timezone("Europe/Berlin")

GCP_PROJECT = Variable.get("GCP_PROJECT", default_var="cocoa-prices-430315")
BQ_LOC = Variable.get("BQ_LOCATION", default_var="europe-west3")
BQ_PROC = Variable.get("BQ_PROC", default_var="analytics_delivery.run_daily_pipeline")
BQ_PROC_FQN = f"{GCP_PROJECT}.{BQ_PROC}"
# Deployment pins the verified routine body before installing this DAG.
RELEASE_PROCEDURE_SHA256 = "4da1ef0d67ac92cc9e77462f2451de558ea74ad6f786b02a396128326ecfefe7"

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

# Receipt/staging completion, not an increasing timestamp or physical row count.
PROGRESS_TIMEOUT_MIN = int(Variable.get("PROGRESS_TIMEOUT_MIN", default_var="10"))
PROGRESS_POLL_SEC = int(Variable.get("PROGRESS_POLL_SEC", default_var="20"))
COVERAGE_BUCKET = Variable.get("COVERAGE_RECEIPT_BUCKET", default_var="raw-historic-data")
if not 1 <= PROGRESS_TIMEOUT_MIN <= 10 or not 1 <= PROGRESS_POLL_SEC <= 60:
    raise ValueError("Daily completion timeout must be 1-10 minutes and polling 1-60 seconds")
SOURCE_CONTRACT = {
    "cocoa": {"source": "ICCO", "measurements": ("cocoa_price",), "maximum_age": 7},
    "oil": {"source": "EIA", "measurements": ("oil_price",), "maximum_age": 14},
    "weather": {"source": "NASA_POWER", "measurements": ("precipitation", "soil_moisture"), "maximum_age": 7},
}

# Producer triggers
COCOA_FN_URL = Variable.get("COCOA_FN_URL", default_var=None)
OIL_TRIGGER_TOPIC = Variable.get("OIL_TRIGGER_TOPIC", default_var="eia-oil-trigger")
WEATHER_TRIGGER_TOPIC = Variable.get(
    "WEATHER_TRIGGER_TOPIC", default_var="weather-trigger"
)

# Feed registry. Daily consumers run independently of this VM on Cloud Run.
FEEDS = [
    {
        "key": "cocoa",
        "table": BQ_STG["cocoa"],
        "url": COCOA_FN_URL,
        "trigger_topic": None,
    },
    {
        "key": "oil",
        "table": BQ_STG["oil"],
        "url": None,
        "trigger_topic": OIL_TRIGGER_TOPIC,
    },
    {
        "key": "weather",
        "table": BQ_STG["weather"],
        "url": None,
        "trigger_topic": WEATHER_TRIGGER_TOPIC,
    },
]


# ===== Helpers =====
def _utc(value):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("Acquisition timestamps must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _run_scope(dag_run):
    start = _utc(dag_run.start_date)
    engineering = (dag_run.conf or {}).get("engineering_validation", False)
    if type(engineering) is not bool:
        raise AirflowFailException("engineering_validation must be an explicit boolean")
    kind = getattr(dag_run.run_type, "value", dag_run.run_type)
    if kind not in {"manual", "scheduled"}:
        raise AirflowFailException("Unsupported daily acquisition run type")
    scheduled = kind == "scheduled"
    business = (_utc(dag_run.data_interval_end) if scheduled else start).astimezone(TZ).date()
    window = pendulum.datetime(business.year, business.month, business.day, 18, 30, tz=TZ)
    if not engineering and (business.weekday() > 4 or start < window or start.astimezone(TZ).date() != business):
        raise AirflowFailException("Acquisition is outside its business-date service window; use a new recovery run")
    return {"business_date": business.isoformat(), "run_started_at": start.isoformat(),
            "not_before": (start if engineering else max(start, window)).isoformat(),
            "engineering_validation": engineering}


def capture_baseline_fn(ti, dag_run):
    # Preserve the installed task name; its baseline is now an intended run scope.
    ti.xcom_push(key="baseline", value=_run_scope(dag_run))


def _expected_acquisition(feed_key, dag_run):
    """Identity v1, including the pinned Functions Framework 3.8.3 Pub/Sub scope."""
    feed = next(f for f in FEEDS if f["key"] == feed_key)
    scope = None
    if feed["trigger_topic"]:
        scope = {"service": "pubsub.googleapis.com",
                 "name": f"projects/{GCP_PROJECT}/topics/{feed['trigger_topic']}",
                 "type": "type.googleapis.com/google.pubsub.v1.PubsubMessage"}
    key = f"{dag_run.dag_id}:{dag_run.run_id}:{feed_key}"
    return hashlib.sha256(json.dumps([SOURCE_CONTRACT[feed_key]["source"], scope, key],
                                    sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _invoke_producer(
    feed_key: str, url: str | None = None, trigger_topic: str | None = None, dag_run=None
):
    acquisition_key = f"{dag_run.dag_id}:{dag_run.run_id}:{feed_key}" if dag_run is not None else uuid.uuid4().hex
    invocation_started_at = datetime.now(timezone.utc).isoformat()
    if trigger_topic:
        try:
            hook = PubSubHook(gcp_conn_id="gcp_default")
            publisher = hook.get_conn()
            topic_path = publisher.topic_path(GCP_PROJECT, trigger_topic)
            publisher.publish(topic_path, b"{}", acquisition_key=acquisition_key).result(timeout=30)
        except Exception:
            raise AirflowException(f"{feed_key}: trigger enqueue failed") from None
        return {"invoked": True, "topic": trigger_topic, "invocation_started_at": invocation_started_at}

    if not url:
        raise AirflowFailException(f"{feed_key}: no acquisition endpoint configured")

    import requests as rq
    from google.auth.transport.requests import Request
    from google.oauth2 import id_token

    try:
        token = id_token.fetch_id_token(Request(), url)
        headers = {"Authorization": f"Bearer {token}", "X-Acquisition-Key": acquisition_key}

        # Read-only readiness probe; a missing endpoint fails permanently.
        try:
            r = rq.head(url, headers=headers, timeout=(5, 10))
            if r.status_code == 404:
                raise AirflowFailException(f"{feed_key}: endpoint not found")
        except AirflowFailException:
            raise
        except Exception:
            pass

        # single invocation (POST only)
        resp = rq.post(url, headers=headers, timeout=(5, 65))
        if resp.status_code == 404:
            raise AirflowFailException(f"{feed_key}: endpoint not found")
        resp.raise_for_status()
        return {"invoked": True, "status": resp.status_code, "invocation_started_at": invocation_started_at}
    except AirflowFailException:
        raise
    except Exception:
        raise AirflowException(f"{feed_key}: producer invocation failed") from None


def _check_timeout(deadline):
    budget = deadline - time.monotonic()
    if budget <= 0:
        raise AirflowException("Source completion check exceeded its request budget")
    return min(5, budget)


def _receipt_document(payload, identity, feed_key, scope, invoked_at):
    """Validate an actual requested inventory, not a synthetic daily calendar."""
    contract = SOURCE_CONTRACT[feed_key]
    try:
        document = json.loads(payload)
        if (document["version"] != 1 or document["source"] != contract["source"]
                or document["stage"] != "publish_acknowledged" or document["acquisition_id"] != identity):
            raise ValueError("Wrong completion contract")
        acquired = _utc(document["acquired_at"])
        if acquired < max(_utc(scope["not_before"]), _utc(invoked_at)):
            return None  # A prior trigger attempt cannot establish new acquisition.
        business = date.fromisoformat(scope["business_date"])
        if acquired > datetime.now(timezone.utc) + timedelta(minutes=5) or acquired.astimezone(TZ).date() != business:
            raise ValueError("Acquisition is outside its business date")
        start, end = (date.fromisoformat(document[k]) for k in ("window_start", "window_end"))
        if (start.isoformat() != document["window_start"] or end.isoformat() != document["window_end"]
                or not 30 <= (end-start).days < 90 or end != acquired.date() or end > business):
            raise ValueError("Wrong acquisition window")
        inventory = document["observations"]
        if (not isinstance(inventory, list) or type(document["published"]) is not int
                or document["published"] != len(inventory) or len(inventory) > 90
                or document["status"] != ("published" if inventory else "no_observations")):
            raise ValueError("Incomplete publication receipt")
        expected = {}
        for observation in inventory:
            observed = date.fromisoformat(observation["date"])
            missing = observation["missing_measurements"]
            if (observed.isoformat() != observation["date"] or not start <= observed <= end
                    or observation["date"] in expected
                    or not re.fullmatch(r"[0-9a-f]{64}", observation["observation_id"])
                    or not isinstance(missing, list) or len(set(missing)) != len(missing)
                    or not set(missing) <= set(contract["measurements"])):
                raise ValueError("Invalid observation inventory")
            expected[observation["date"]] = observation
        bounds = (min(expected), max(expected)) if expected else (None, None)
        if bounds != (document["observation_start"], document["observation_end"]):
            raise ValueError("Incorrect observation bounds")
        absent = document["not_exposed_dates"]
        calendar = {(start + timedelta(days=i)).isoformat() for i in range((end-start).days+1)}
        if (not isinstance(absent, list) or len(set(absent)) != len(absent)
                or set(absent) & set(expected) or set(absent) | set(expected) != calendar
                or (feed_key == "weather" and absent)):
            raise ValueError("Requested dates are unaccounted for")
        return document
    except (KeyError, ValueError, TypeError, OverflowError):
        raise AirflowFailException(f"{feed_key}: invalid acquisition completion receipt") from None


def _latest_completion(feed_key, identity, scope, invoked_at, deadline):
    prefix = f"acquisition-coverage/v1/{SOURCE_CONTRACT[feed_key]['source']}/receipts/{identity}/"
    try:
        client = GCSHook(gcp_conn_id="gcp_default").get_conn()
        blobs = list(client.list_blobs(COVERAGE_BUCKET, prefix=prefix, max_results=33, page_size=33,
                                       timeout=_check_timeout(deadline), retry=None))
        if len(blobs) > 32:
            raise AirflowFailException(f"{feed_key}: excessive completed acquisition attempts")
        candidates = []
        for blob in blobs:
            if not re.fullmatch(re.escape(prefix) + r"[0-9a-f]{64}\.json", blob.name) or blob.size is None or blob.size > 262_144:
                raise AirflowFailException(f"{feed_key}: unexpected completion object")
            payload = blob.download_as_bytes(if_generation_match=blob.generation, timeout=_check_timeout(deadline), retry=None)
            digest = hashlib.sha256(payload).hexdigest()
            if not blob.name.endswith("/" + digest + ".json"):
                raise AirflowFailException(f"{feed_key}: completion checksum mismatch")
            document = _receipt_document(payload, identity, feed_key, scope, invoked_at)
            if document:
                candidates.append((_utc(document["acquired_at"]), document, blob.name, str(blob.generation), digest))
        if not candidates:
            return None
        candidates.sort(key=lambda item: item[0], reverse=True)
        if len(candidates) > 1 and candidates[0][0] == candidates[1][0]:
            raise AirflowFailException(f"{feed_key}: conflicting completion timestamps")
        return candidates[0]
    except AirflowFailException:
        raise
    except Exception:
        raise AirflowException(f"{feed_key}: completion receipt read failed") from None


def _source_ages(feed_key, document, scope):
    business = date.fromisoformat(scope["business_date"])
    contract = SOURCE_CONTRACT[feed_key]
    dates = {}
    for measurement in contract["measurements"]:
        available = [date.fromisoformat(row["date"]) for row in document["observations"]
                     if measurement not in row["missing_measurements"]]
        if not available:
            raise AirflowFailException(f"{feed_key}: no valid {measurement} in the acquired window")
        latest = max(available)
        age = (business-latest).days
        if age < 0 or age > contract["maximum_age"]:
            raise AirflowFailException(f"{feed_key}: {measurement} source age {age} exceeds {contract['maximum_age']} days")
        dates[measurement] = {"observation_date": latest.isoformat(), "age_days": age}
    return dates


def _staged_inventory(feed_key, table_fqn, identity, acquired_at, invoked_at, deadline):
    if not re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+", table_fqn):
        raise AirflowFailException(f"{feed_key}: invalid staging table configuration")
    from google.cloud import bigquery

    nulls = ",".join(f"LOGICAL_OR({m} IS NULL) {m}_missing_any,LOGICAL_AND({m} IS NULL) {m}_missing_all"
                     for m in SOURCE_CONTRACT[feed_key]["measurements"])
    sql = f"""SELECT date,observation_id,
        COUNTIF(timestamp_status IS DISTINCT FROM 'valid' OR consumer_received_at IS NULL
                OR consumer_received_at<@invoked_at) provenance_errors,{nulls}
        FROM `{table_fqn}` WHERE acquisition_id=@acquisition_id AND producer_acquired_at=@acquired_at
        GROUP BY date,observation_id"""
    config = bigquery.QueryJobConfig(use_legacy_sql=False, maximum_bytes_billed=1_073_741_824, query_parameters=[
        bigquery.ScalarQueryParameter("acquisition_id", "STRING", identity),
        bigquery.ScalarQueryParameter("acquired_at", "TIMESTAMP", _utc(acquired_at)),
        bigquery.ScalarQueryParameter("invoked_at", "TIMESTAMP", _utc(invoked_at)),
    ])
    try:
        client = BigQueryHook(gcp_conn_id="gcp_default", location=BQ_LOC, use_legacy_sql=False).get_client(project_id=GCP_PROJECT, location=BQ_LOC)
        job = client.query(sql, job_config=config, timeout=_check_timeout(deadline), retry=None)
        return list(job.result(max_results=181, timeout=min(20, max(0.1, deadline-time.monotonic())), retry=None))
    except Exception:
        raise AirflowException(f"{feed_key}: staging completion query failed") from None


def make_wait_callable(feed_key: str, table_fqn: str):
    def _wait_once(**ctx):
        ti, dag_run = ctx["ti"], ctx["dag_run"]
        scope = ti.xcom_pull(task_ids="capture_baseline", key="baseline")
        trigger = ti.xcom_pull(task_ids=f"trigger_{feed_key}") or {}
        if not scope or not trigger.get("invoked") or not trigger.get("invocation_started_at"):
            raise AirflowFailException(f"{feed_key}: current acquisition scope/trigger receipt is missing")
        identity = _expected_acquisition(feed_key, dag_run)
        deadline = time.monotonic()+45
        completion = _latest_completion(feed_key, identity, scope, trigger["invocation_started_at"], deadline)
        if not completion:
            ti.log.info("Feed %s: awaiting current acquisition completion (%s)", feed_key, identity)
            return False
        _, document, object_name, generation, digest = completion
        ages = _source_ages(feed_key, document, scope)
        rows = _staged_inventory(feed_key, table_fqn, identity, document["acquired_at"], trigger["invocation_started_at"], deadline)
        expected = {(r["date"], r["observation_id"]): r for r in document["observations"]}
        found = set()
        for row in rows:
            key = (str(row["date"]), row["observation_id"])
            if key not in expected or row["provenance_errors"]:
                raise AirflowFailException(f"{feed_key}: staging contradicts the completed acquisition")
            for measurement in SOURCE_CONTRACT[feed_key]["measurements"]:
                missing = measurement in expected[key]["missing_measurements"]
                if row[measurement+"_missing_any"] != missing or row[measurement+"_missing_all"] != missing:
                    raise AirflowFailException(f"{feed_key}: staged missingness contradicts source evidence")
            found.add(key)
        if found != set(expected):
            ti.log.info("Feed %s: complete source inventory=%s, staged revisions=%s", feed_key, len(expected), len(found))
            return False
        ti.xcom_push(key="accepted_source", value={"acquisition_id": identity, "acquired_at": document["acquired_at"],
            "receipt_object": object_name, "receipt_generation": generation, "receipt_sha256": digest,
            "window_start": document["window_start"], "window_end": document["window_end"],
            "observations": len(expected), "observation_manifest": document["observations"],
            "source_ages": ages, "engineering_validation": scope["engineering_validation"]})
        return True

    def _wait_callable(**ctx):
        try:
            return _wait_once(**ctx)
        except AirflowFailException:
            raise
        except AirflowException:
            ctx["ti"].log.warning("Feed %s: completion IO unavailable; retrying within the sensor deadline", feed_key)
            return False

    return _wait_callable


def _release_request(ti, dag_run, procedure_body):
    code_id = hashlib.sha256(procedure_body.encode()).hexdigest()
    if code_id != RELEASE_PROCEDURE_SHA256:
        raise AirflowFailException("Release procedure differs from the verified deployment pin")
    if not re.fullmatch(r"[a-z][a-z0-9-]+\.[A-Za-z_][A-Za-z0-9_]*\.run_daily_pipeline", BQ_PROC_FQN):
        raise AirflowFailException("Invalid release procedure configuration")
    scope = ti.xcom_pull(task_ids="capture_baseline", key="baseline")
    if not scope:
        raise AirflowFailException("Current release scope is missing")
    accepted = {}
    for feed in SOURCE_CONTRACT:
        source = ti.xcom_pull(task_ids=f"wait_{feed}_rows", key="accepted_source")
        if (not source or source.get("acquisition_id") != _expected_acquisition(feed, dag_run)
                or _utc(source["acquired_at"]) < _utc(scope["run_started_at"])):
            raise AirflowFailException(f"{feed}: accepted current acquisition is missing")
        accepted[feed] = source
    release = hashlib.sha256(json.dumps([dag_run.dag_id, dag_run.run_id], separators=(",", ":")).encode()).hexdigest()
    values = [
        ("release", "STRING", release), ("business", "DATE", scope["business_date"]),
        ("run", "STRING", dag_run.run_id), ("started", "TIMESTAMP", scope["run_started_at"]),
        ("sources", "STRING", json.dumps(accepted, sort_keys=True, separators=(",", ":"))),
        ("code", "STRING", code_id), ("stage", "STRING", None),
    ]
    return {"release_id": release, "code_id": code_id,
            "query": f"CALL `{BQ_PROC_FQN}`(@release,@business,@run,@started,@sources,@code,@stage)",
            "parameters": [{"name": name, "parameterType": {"type": kind},
                            "parameterValue": {"value": value}} for name, kind, value in values]}


def _same_release_job(actual, request):
    def parameters(items):
        result = {}
        for item in items:
            name, kind = item["name"], item["parameterType"]["type"]
            if name in result:
                return None
            value = item.get("parameterValue", {}).get("value")
            if value is not None and kind == "TIMESTAMP":
                value = _utc(value).isoformat()
            result[name] = (kind, value)
        return result
    return actual["query"] == request["query"] and parameters(actual.get("queryParameters", [])) == parameters(request["parameters"])


def _run_release_job(client, request):
    from google.api_core.exceptions import Conflict, NotFound
    from google.cloud import bigquery

    # GET job metadata omits the value key for SQL NULL; jobs.insert needs it.
    parameters = [{**p, "parameterValue": {**p.get("parameterValue", {}),
                   "value": p.get("parameterValue", {}).get("value")}} for p in request["parameters"]]
    config = bigquery.QueryJobConfig.from_api_repr({
        "jobTimeoutMs": "1200000",
        "labels": {"app": "cocoa-prices", "component": "atomic-release", "release": request["release_id"][:32]},
        "query": {"queryParameters": parameters, "useLegacySql": False, "maximumBytesBilled": "1073741824"},
    })
    retryable = {"backendError", "internalError", "rateLimitExceeded", "jobRateLimitExceeded"}
    for attempt in range(2):
        job_id = "cocoa_release_"+request["release_id"]+"_"+str(attempt)
        try:
            job = client.get_job(job_id, location=BQ_LOC, retry=None, timeout=30)
        except NotFound:
            try:
                job = client.query(request["query"], job_config=config, job_id=job_id,
                                   location=BQ_LOC, retry=None, job_retry=None, timeout=30)
            except Conflict:
                job = client.get_job(job_id, location=BQ_LOC, retry=None, timeout=30)
            except Exception:
                raise AirflowException(f"Release submission uncertain; reconcile {job_id}") from None
        except Exception:
            raise AirflowException(f"Release lookup unavailable; retain {job_id}") from None
        actual = job.to_api_repr()["configuration"]["query"]
        if not _same_release_job(actual, request):
            raise AirflowFailException("Release job identity is already bound to a different request")
        if job.state == "DONE" and job.error_result:
            if job.error_result.get("reason") in retryable:
                continue
            raise AirflowFailException(f"Release job failed its contract: {job_id}")
        try:
            rows = list(job.result(timeout=1260, job_retry=None))
        except Exception:
            raise AirflowException(f"Release did not confirm completion; reattach {job_id}") from None
        if len(rows) != 1 or rows[0]["release_id"] != request["release_id"] or rows[0]["status"] != "published":
            raise AirflowFailException("Release result does not confirm a completed publication")
        return {**{key: (value.isoformat() if isinstance(value, (date, datetime)) else value)
                   for key, value in dict(rows[0]).items()}, "job_id": job_id, "code_id": request["code_id"]}
    raise AirflowFailException("Release exhausted its two confirmed execution attempts")


def run_release_fn(ti, dag_run):
    client = BigQueryHook(gcp_conn_id="gcp_default", location=BQ_LOC).get_client(project_id=GCP_PROJECT, location=BQ_LOC)
    try:
        body = client.get_routine(BQ_PROC_FQN, retry=None, timeout=30).body
    except Exception:
        raise AirflowException("Verified release procedure is unavailable") from None
    request = _release_request(ti, dag_run, body)
    ti.xcom_push(key="release_request", value={"release_id": request["release_id"], "code_id": request["code_id"]})
    result = _run_release_job(client, request)
    ti.log.info("Published release %s with model %s in job %s", result["release_id"], result["model_id"], result["job_id"])
    return result


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

    wait_tasks = []

    for f in FEEDS:
        # 1) Trigger the producer via HTTP or its Eventarc Pub/Sub topic.
        trigger = PythonOperator(
            task_id=f"trigger_{f['key']}",
            python_callable=_invoke_producer,
            op_kwargs={"feed_key": f["key"], "url": f["url"], "trigger_topic": f["trigger_topic"]},
            execution_timeout=timedelta(minutes=3),
            retries=2,
            retry_delay=timedelta(seconds=30),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=2),
        )

        # 2) Require the exact run/attempt inventory and source-age contract.
        wait_rows = PythonSensor(
            task_id=f"wait_{f['key']}_rows",
            python_callable=make_wait_callable(f["key"], f["table"]),
            poke_interval=PROGRESS_POLL_SEC,
            timeout=PROGRESS_TIMEOUT_MIN * 60,
            mode="reschedule",
            soft_fail=False,
            execution_timeout=timedelta(minutes=1),
            retries=0,  # IO is repolled inside the one overall sensor timeout.
        )

        # Managed Pub/Sub push consumers require no in-DAG SSH lifecycle.
        capture_baseline >> trigger >> wait_rows

        wait_tasks.append(wait_rows)

    run_bq_pipeline = PythonOperator(
        task_id="run_bq_pipeline",
        python_callable=run_release_fn,
        execution_timeout=timedelta(minutes=22),
        retries=1,
        retry_delay=timedelta(seconds=30),
        trigger_rule=TriggerRule.ALL_SUCCESS,
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
    )

    chain(wait_tasks, run_bq_pipeline, summary)
