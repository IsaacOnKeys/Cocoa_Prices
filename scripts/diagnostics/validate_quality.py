"""Exercise the exact recurring SQL checks against bounded disposable candidate fixtures."""

import argparse
import hashlib
import json
import re
import runpy
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROJECT = "cocoa-prices-430315"
RENDER = runpy.run_path(str(ROOT / "scripts/deploy/render_delivery_sql.py"))["render"]

FIXTURE = """
DECLARE p_release_id STRING DEFAULT @release;
DECLARE p_business_date DATE DEFAULT DATE '2014-04-01';
DECLARE p_run_started_at TIMESTAMP DEFAULT TIMESTAMP '2014-04-01 18:00:00+00';
DECLARE input_cutoff TIMESTAMP DEFAULT TIMESTAMP '2014-04-01 18:05:00+00';
DECLARE p_sources JSON;
DECLARE quality JSON;
CREATE TEMP TABLE base_data AS
 SELECT d AS date,i,2000.0+10*i AS cocoa_price,10.0+0.25*i AS oil_price,
   1.0+0.01*i AS precipitation,0.2+0.001*i AS soil_moisture
 FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2014-01-02',p_business_date)) d WITH OFFSET i;
CREATE TEMP TABLE source_input AS
 SELECT kind,feed,b.date,IF(feed='cocoa',cocoa_price,IF(feed='oil',oil_price,NULL)) AS price,
   IF(feed='weather',precipitation,NULL) AS precipitation,IF(feed='weather',soil_moisture,NULL) AS soil_moisture,
   IF(kind='stage',LOWER(TO_HEX(SHA256(feed))),NULL) AS acquisition_id,
   IF(kind='stage',LOWER(TO_HEX(SHA256(CONCAT(feed,CAST(b.date AS STRING))))),NULL) AS observation_id,
   IF(kind='stage',TIMESTAMP '2014-04-01 18:00:01+00',NULL) AS producer_acquired_at,
   IF(kind='stage',TIMESTAMP '2014-04-01 18:00:03+00',NULL) AS consumer_received_at,
   IF(kind='stage',TIMESTAMP '2014-04-01 18:00:02+00',NULL) AS source_published_at,
   IF(kind='stage','valid',NULL) AS timestamp_status
 FROM base_data b CROSS JOIN UNNEST(['cocoa','oil','weather']) feed CROSS JOIN UNNEST(['baseline','stage']) kind
 WHERE (kind='baseline' AND b.date<p_business_date) OR (kind='stage' AND b.date>=DATE_SUB(p_business_date,INTERVAL 30 DAY));
CREATE TEMP TABLE candidate_joined AS SELECT * EXCEPT(i) FROM base_data;
CREATE TEMP TABLE candidate_features AS
 SELECT date,cocoa_price AS y,
   IF(i>=1,2000.0+10*(i-1),NULL) AS y_lag1,
   IF(i>=7,2000.0+10*(i-7),NULL) AS y_lag7,
   IF(i>=28,2000.0+10*(i-28),NULL) AS y_lag28,
   10.0+0.25*(i-LEAST(i,6)/2.0) AS oil_ma7,
   1.0+0.01*(i-LEAST(i,6)/2.0) AS precip_ma7,
   0.2+0.001*(i-LEAST(i,6)/2.0) AS soil_ma7,
   CAST(EXTRACT(DAYOFWEEK FROM date) AS STRING) AS dow,CAST(EXTRACT(MONTH FROM date) AS STRING) AS mon,
   IF(DATE_ADD(date,INTERVAL 40 DAY)<=p_business_date,2000.0+10*(i+40),NULL) AS target_40
 FROM base_data;
CREATE TEMP TABLE candidate_forecast AS
 SELECT d AS forecast_date,2000.0+10*DATE_DIFF(d,DATE '2014-01-02',DAY)+5 AS forecast_cocoa_price
 FROM UNNEST(GENERATE_DATE_ARRAY(DATE_ADD(p_business_date,INTERVAL 1 DAY),DATE_ADD(p_business_date,INTERVAL 40 DAY))) d;
CREATE TEMP TABLE candidate_predictions AS
 SELECT date AS prediction_date,DATE_ADD(date,INTERVAL 40 DAY) AS forecast_date,
   2000.0+10*(i+40)+5 AS yhat_40,
   IF(DATE_ADD(date,INTERVAL 40 DAY)<=p_business_date,2000.0+10*(i+40),NULL) AS actual FROM base_data;
"""

MANIFEST = """
CREATE OR REPLACE TEMP TABLE fixture_manifests AS
 SELECT feed,ARRAY_AGG(STRUCT(date,observation_id,
   CASE feed WHEN 'cocoa' THEN IF(price IS NULL,['cocoa_price'],ARRAY<STRING>[])
     WHEN 'oil' THEN IF(price IS NULL,['oil_price'],ARRAY<STRING>[])
     ELSE ARRAY_CONCAT(IF(precipitation IS NULL,['precipitation'],ARRAY<STRING>[]),
       IF(soil_moisture IS NULL,['soil_moisture'],ARRAY<STRING>[])) END AS missing_measurements) ORDER BY date) AS observation_manifest
 FROM source_input WHERE kind='stage' GROUP BY feed;
SET p_sources = (SELECT TO_JSON(STRUCT(
 (SELECT AS STRUCT DATE_SUB(p_business_date,INTERVAL 30 DAY) AS window_start,p_business_date AS window_end,
   TIMESTAMP '2014-04-01 18:00:01+00' AS acquired_at,31 AS observations,observation_manifest FROM fixture_manifests WHERE feed='cocoa') AS cocoa,
 (SELECT AS STRUCT DATE_SUB(p_business_date,INTERVAL 30 DAY) AS window_start,p_business_date AS window_end,
   TIMESTAMP '2014-04-01 18:00:01+00' AS acquired_at,31 AS observations,observation_manifest FROM fixture_manifests WHERE feed='oil') AS oil,
 (SELECT AS STRUCT DATE_SUB(p_business_date,INTERVAL 30 DAY) AS window_start,p_business_date AS window_end,
   TIMESTAMP '2014-04-01 18:00:01+00' AS acquired_at,31 AS observations,observation_manifest FROM fixture_manifests WHERE feed='weather') AS weather)));
"""

# Each case mutates data independently of the checker expressions.
CASES = [
    ("complete", "", False, False, None),
    (
        "missing_source_revision",
        "DELETE source_input WHERE kind='stage' AND feed='cocoa' AND date=p_business_date;",
        False,
        False,
        "source_window_reconciliation",
    ),
    (
        "duplicate_source_key",
        "INSERT source_input SELECT * FROM source_input WHERE kind='stage' AND feed='oil' AND date=p_business_date;",
        False,
        False,
        "frozen_source_keys",
    ),
    (
        "wrong_revision_identity",
        "UPDATE source_input SET observation_id=REPEAT('0',64) WHERE kind='stage' AND feed='cocoa' AND date=p_business_date;",
        False,
        False,
        "source_window_reconciliation",
    ),
    (
        "null_conflicts_with_receipt",
        "UPDATE source_input SET price=NULL WHERE kind='stage' AND feed='oil' AND date=p_business_date;",
        False,
        False,
        "source_missingness",
    ),
    (
        "null_date",
        "UPDATE source_input SET date=NULL WHERE kind='stage' AND feed='oil' AND date=p_business_date;",
        False,
        False,
        "frozen_dates",
    ),
    (
        "future_date",
        "UPDATE source_input SET date=DATE_ADD(date,INTERVAL 1 DAY) WHERE kind='stage' AND feed='oil' AND date=p_business_date;",
        False,
        False,
        "frozen_dates",
    ),
    (
        "nonfinite_value",
        "UPDATE source_input SET price=CAST('NaN' AS FLOAT64) WHERE kind='stage' AND feed='cocoa' AND date=p_business_date;",
        False,
        False,
        "finite_measurements",
    ),
    (
        "invalid_cocoa_domain",
        "UPDATE source_input SET price=0 WHERE kind='stage' AND feed='cocoa' AND date=p_business_date;",
        False,
        False,
        "measurement_domains",
    ),
    (
        "negative_rainfall",
        "UPDATE source_input SET precipitation=-0.1 WHERE kind='stage' AND feed='weather' AND date=p_business_date;",
        False,
        False,
        "measurement_domains",
    ),
    (
        "invalid_wetness",
        "UPDATE source_input SET soil_moisture=1.1 WHERE kind='stage' AND feed='weather' AND date=p_business_date;",
        False,
        False,
        "measurement_domains",
    ),
    (
        "explicit_missing_allowed",
        "UPDATE source_input SET soil_moisture=NULL WHERE kind='stage' AND feed='weather' AND date=p_business_date;",
        True,
        True,
        None,
    ),
    (
        "legacy_sentinel_allowed",
        "UPDATE source_input SET soil_moisture=-999.0 WHERE kind='baseline' AND feed='weather' AND date=DATE '2014-01-02';",
        False,
        True,
        None,
    ),
    (
        "finite_negative_oil_allowed",
        "UPDATE source_input SET price=-1.0 WHERE kind='stage' AND feed='oil' AND date=p_business_date;",
        False,
        True,
        None,
    ),
    (
        "stale_weather_complete_calendar",
        "UPDATE source_input SET precipitation=NULL,soil_moisture=NULL WHERE kind='stage' AND feed='weather' AND date>DATE_SUB(p_business_date,INTERVAL 8 DAY);",
        True,
        False,
        "source_age_weather_precipitation",
    ),
    (
        "wrong_joined_source_value",
        "UPDATE candidate_joined SET cocoa_price=cocoa_price+1 WHERE date=p_business_date;",
        False,
        False,
        "joined_source_alignment",
    ),
    (
        "wrong_price_lag",
        "UPDATE candidate_features SET y_lag7=y_lag7+1 WHERE date=p_business_date;",
        False,
        False,
        "feature_lags_and_label",
    ),
    (
        "wrong_label",
        "UPDATE candidate_features SET target_40=target_40+1 WHERE date=DATE '2014-01-02';",
        False,
        False,
        "feature_lags_and_label",
    ),
    (
        "wrong_rolling_mean",
        "UPDATE candidate_features SET oil_ma7=oil_ma7+0.001 WHERE date=p_business_date;",
        False,
        False,
        "feature_rolling_means",
    ),
    (
        "wrong_calendar_category",
        "UPDATE candidate_features SET mon='0' WHERE date=p_business_date;",
        False,
        False,
        "feature_calendar_categories",
    ),
    (
        "missing_forecast",
        "DELETE candidate_forecast WHERE forecast_date=DATE_ADD(p_business_date,INTERVAL 1 DAY);",
        False,
        False,
        "forecast_count",
    ),
    (
        "duplicate_forecast_date",
        "UPDATE candidate_forecast SET forecast_date=DATE_ADD(p_business_date,INTERVAL 1 DAY) WHERE forecast_date=DATE_ADD(p_business_date,INTERVAL 2 DAY);",
        False,
        False,
        "forecast_unique_dates",
    ),
    (
        "invalid_forecast_value",
        "UPDATE candidate_forecast SET forecast_cocoa_price=CAST('NaN' AS FLOAT64) WHERE forecast_date=DATE_ADD(p_business_date,INTERVAL 1 DAY);",
        False,
        False,
        "forecast_values",
    ),
    (
        "wrong_historical_horizon",
        "UPDATE candidate_predictions SET forecast_date=DATE_ADD(prediction_date,INTERVAL 39 DAY) WHERE prediction_date=DATE '2014-01-02';",
        False,
        False,
        "historical_score_keys",
    ),
    (
        "wrong_historical_actual",
        "UPDATE candidate_predictions SET actual=actual+1 WHERE prediction_date=DATE '2014-01-02';",
        False,
        False,
        "historical_actual_alignment",
    ),
    (
        "invalid_historical_metric",
        "UPDATE candidate_predictions SET yhat_40=CAST('Infinity' AS FLOAT64) WHERE prediction_date=DATE '2014-01-02';",
        False,
        False,
        "historical_metric_finite",
    ),
    (
        "gross_forecast_scale_warning",
        "UPDATE candidate_forecast SET forecast_cocoa_price=forecast_cocoa_price*1000 WHERE TRUE;",
        False,
        False,
        None,
    ),
]
CASES.append(
    (
        "same_count_wrong_historical_origins",
        "UPDATE candidate_predictions SET prediction_date=DATE_ADD(p_business_date,INTERVAL 1 DAY), "
        "forecast_date=DATE_ADD(p_business_date,INTERVAL 41 DAY),actual=NULL WHERE prediction_date=DATE '2014-01-02';",
        False,
        False,
        "historical_origin_keys",
    )
)

CASES.append(
    (
        "expired_historical_fill_rejected",
        "DELETE source_input WHERE feed='cocoa' AND date BETWEEN DATE '2014-01-10' AND DATE '2014-01-25';"
        "UPDATE candidate_joined SET cocoa_price=2070 WHERE date BETWEEN DATE '2014-01-10' AND DATE '2014-01-25';",
        False,
        False,
        "joined_source_alignment",
    )
)


def names(token):
    if not re.fullmatch(r"20[0-9]{6}_[a-f0-9]{8}", token):
        raise ValueError("Invalid quality validation namespace")
    return "quality_validation_" + token


def client():
    from google.cloud import bigquery
    from validate_ingestion import credentials

    return bigquery.Client(
        project=PROJECT, credentials=credentials(), location="europe-west3"
    )


def checked(path):
    state = json.loads(path.read_text())
    if state["dataset"] != PROJECT + "." + names(state["token"]):
        raise ValueError("Only isolated quality validation datasets are allowed")
    return state


def exercise(path):
    from google.api_core.exceptions import BadRequest
    from google.cloud import bigquery

    if path.exists():
        raise ValueError("Refusing to overwrite a quality validation receipt")
    token = datetime.now(timezone.utc).strftime("%Y%m%d") + "_" + uuid.uuid4().hex[:8]
    dataset = names(token)
    state = {
        "token": token,
        "dataset": PROJECT + "." + dataset,
        "phase": "preparing",
        "cases": {},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2))
    bq = client()
    ds = bigquery.Dataset(state["dataset"])
    ds.location = "europe-west3"
    ds.default_table_expiration_ms = 86400000
    ds.labels = {"app": "cocoa-prices", "purpose": "quality-validation", "token": token}
    bq.create_dataset(ds, timeout=30)
    bq.create_table(
        bigquery.Table(
            state["dataset"] + ".release_runs",
            schema=[
                bigquery.SchemaField("release_id", "STRING"),
                bigquery.SchemaField("business_date", "DATE"),
                bigquery.SchemaField("quality_results", "JSON"),
            ],
        ),
        timeout=30,
    )
    context = {
        "delivery": dataset,
        "curated": dataset + "_unused_curated",
        "staging": dataset + "_unused_staging",
        "models": dataset,
        "studio": dataset,
    }
    input_sql = RENDER("input_quality_checks", **context)
    output_sql = RENDER("output_quality_checks", **context)
    for name, mutation, rebuild_manifest, input_only, expected_error in CASES:
        release = hashlib.sha256((token + "/" + name).encode()).hexdigest()
        sql = FIXTURE + MANIFEST + mutation + (MANIFEST if rebuild_manifest else "")
        sql += f"INSERT `{state['dataset']}.release_runs` VALUES(p_release_id,p_business_date,NULL);"
        sql += input_sql + ("" if input_only else output_sql)
        config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("release", "STRING", release)
            ],
            maximum_bytes_billed=1073741824,
            labels={"app": "cocoa-prices", "purpose": "quality-validation"},
        )
        job = bq.query(sql, job_config=config)
        state["cases"][name] = {"job_id": job.job_id, "expected_error": expected_error}
        path.write_text(json.dumps(state, indent=2))
        failed = False
        try:
            list(job.result(timeout=120))
        except BadRequest as error:
            if "quality gate failed" not in str(error):
                raise
            failed = True
        query = f"SELECT quality_results FROM `{state['dataset']}.release_runs` WHERE release_id=@release"
        report = list(bq.query(query, job_config=config).result(timeout=30))[0][
            "quality_results"
        ]
        errors = [
            r["check_name"]
            for r in report["checks"]
            if r["severity"] == "error" and r["violations"]
        ]
        warnings = [
            r["check_name"]
            for r in report["checks"]
            if r["severity"] == "warning" and r["violations"]
        ]
        assert failed == bool(expected_error), (name, failed, errors)
        if expected_error:
            assert expected_error in errors, (name, expected_error, errors)
        if name == "gross_forecast_scale_warning":
            assert "forecast_scale" in warnings
        state["cases"][name].update(
            verified=True, errors=errors, warnings=warnings, report=report
        )
        path.write_text(json.dumps(state, indent=2))
        print(
            json.dumps(
                {"case": name, "passed": True, "errors": errors, "warnings": warnings}
            ),
            flush=True,
        )
    state["phase"] = "verified"
    path.write_text(json.dumps(state, indent=2))
    print(
        json.dumps({"phase": "verified", "cases": len(CASES), "token": token}),
        flush=True,
    )


def cleanup(path):
    state = checked(path)
    bq = client()
    dataset = bq.get_dataset(state["dataset"], timeout=30)
    if (
        dataset.labels.get("purpose") != "quality-validation"
        or dataset.labels.get("token") != state["token"]
    ):
        raise ValueError("Refusing to delete an unlabelled quality dataset")
    bq.delete_dataset(dataset, delete_contents=True, timeout=60)
    state["resources_deleted"] = True
    path.write_text(json.dumps(state, indent=2))
    print(json.dumps({"resources_deleted": True, "token": state["token"]}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("exercise", "cleanup"))
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    {"exercise": exercise, "cleanup": cleanup}[args.phase](args.receipt)


if __name__ == "__main__":
    main()
