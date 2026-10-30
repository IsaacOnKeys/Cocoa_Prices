"""Actual issued predictions, immutable actual vintages and explicit pending outcomes."""

import json
import math
import re
import runpy
import sqlite3
from pathlib import Path

import pendulum
import pytest

ROOT = Path(__file__).resolve().parents[2]
RENDER = runpy.run_path(str(ROOT/'scripts/deploy/render_delivery_sql.py'))['render']


@pytest.fixture
def db():
    connection = sqlite3.connect(':memory:')
    connection.row_factory = sqlite3.Row
    connection.create_function('BERLIN_START', 1, lambda day:
        pendulum.parse(day, tz='Europe/Berlin').in_timezone('UTC').strftime('%Y-%m-%dT%H:%M:%SZ'))
    connection.create_function('SAFE_DIVIDE', 2, lambda x,y: x/y if x is not None and y else None)
    connection.create_function('SQRT', 1, lambda x: math.sqrt(x) if x is not None else None)
    connection.create_function('POW', 2, lambda x,y: x**y if x is not None else None)
    connection.executescript('''
      CREATE TABLE release_runs(release_id,business_date,status,published_at,input_cutoff,model_id,quality_results);
      CREATE TABLE forecast_40d(release_id,business_date,forecast_date,forecast_cocoa_price);
      CREATE TABLE cocoa(release_id,business_date,Date,Euro_Price);
      CREATE TABLE joined_daily_cocoa_oil_weather(release_id,business_date,date,cocoa_price);
      CREATE TABLE active_release(singleton,release_id);
      INSERT INTO release_runs VALUES('issue','2020-01-01','published','2020-01-01T18:00:00Z','2020-01-01T17:55:00Z','model',
        '{"forecast_product":{"version":"single_40_day_ahead_v1"}}');
      INSERT INTO release_runs VALUES('actual','2020-02-11','published','2020-02-11T18:00:00Z','2020-02-11T17:55:00Z','later','{}');
      INSERT INTO forecast_40d VALUES('issue','2020-01-01','2020-02-10',110);
      INSERT INTO cocoa VALUES('actual','2020-02-11','2020-02-10',120);
      INSERT INTO joined_daily_cocoa_oil_weather VALUES('issue','2020-01-01','2020-01-01',100);
      INSERT INTO joined_daily_cocoa_oil_weather VALUES('issue','2020-01-01','2019-12-30',105);
      INSERT INTO active_release VALUES(1,'actual');
    ''')
    sql = RENDER('issued_evaluation_views').replace('CREATE OR REPLACE VIEW','CREATE VIEW')
    sql = sql.replace('`cocoa-prices-430315.cocoa_related_studio.','`').replace('`cocoa-prices-430315.analytics_delivery.','`')
    sql = sql.replace('JSON_VALUE','JSON_EXTRACT')
    sql = sql.replace('DATE_ADD(f.business_date,INTERVAL 40 DAY)',"DATE(f.business_date,'+40 day')")
    sql = sql.replace('DATE_SUB(i.target_date,INTERVAL 42 DAY)',"DATE(i.target_date,'-42 day')")
    sql = sql.replace("TIMESTAMP(i.target_date,'Europe/Berlin')",'BERLIN_START(i.target_date)')
    sql = sql.replace("SELECT method FROM UNNEST(['model','persistence','weekly_seasonal']) method",
                      "SELECT 'model' AS method UNION ALL SELECT 'persistence' UNION ALL SELECT 'weekly_seasonal'")
    sql = sql.replace("IF(evaluation_status='evaluated',","IIF(evaluation_status='evaluated',")
    sql = re.sub(r"COUNTIF\(evaluation_status='evaluated'\)","SUM(CASE WHEN evaluation_status='evaluated' THEN 1 ELSE 0 END)",sql)
    connection.executescript(sql)
    yield connection
    connection.close()


def test_true_issued_forecast_compares_with_both_paired_baselines(db):
    row = dict(db.execute('SELECT * FROM issued_forecast_evaluation').fetchone())
    assert row['evaluation_status'] == 'evaluated' and row['actual_observed_price'] == 120
    assert row['evaluation_as_of_release_id'] == 'actual'
    metrics = {row['method']: dict(row) for row in db.execute('SELECT * FROM issued_forecast_metrics')}
    assert metrics['model']['mae'] == 10
    assert metrics['persistence']['mae'] == 20 and metrics['weekly_seasonal']['mae'] == 15
    assert {row['scored_count'] for row in metrics.values()} == {1}


def test_old_actual_vintage_remains_reproducible_after_revision(db):
    db.executescript('''
      INSERT INTO release_runs VALUES('revision','2020-02-12','published','2020-02-12T18:00:00Z','2020-02-12T17:55:00Z','newer','{}');
      INSERT INTO cocoa VALUES('revision','2020-02-12','2020-02-10',140);
      UPDATE active_release SET release_id='revision';
    ''')
    assert db.execute('SELECT actual_observed_price FROM issued_forecast_evaluation').fetchone()[0] == 140
    assert db.execute("SELECT actual_observed_price FROM issued_forecast_evaluation_vintages WHERE evaluation_as_of_release_id='actual'").fetchone()[0] == 120


@pytest.mark.parametrize('mutation,status', [
    ("UPDATE active_release SET release_id='issue'",'awaiting_target'),
    ('DELETE FROM cocoa','no_observed_target'),
    ("DELETE FROM joined_daily_cocoa_oil_weather WHERE date='2019-12-30'",'missing_baseline'),
    ("UPDATE release_runs SET published_at='2020-02-10T00:00:00Z' WHERE release_id='issue'",'issued_after_target'),
])
def test_pending_and_ineligible_outcomes_do_not_turn_into_accuracy(db, mutation, status):
    db.execute(mutation)
    assert db.execute('SELECT evaluation_status FROM issued_forecast_evaluation').fetchone()[0] == status
    rows = list(db.execute('SELECT * FROM issued_forecast_metrics'))
    assert len(rows) == 3
    assert all(row['scored_count'] == 0 and row['mae'] is None and row['rmse'] is None for row in rows)


def test_no_new_contract_issues_is_explicit_and_excludes_legacy_rescores(db):
    db.execute('UPDATE release_runs SET quality_results=?', (json.dumps({}),))
    rows = list(db.execute('SELECT * FROM issued_forecast_metrics'))
    assert len(rows) == 3
    assert all(row['issued_count'] == 0 and row['evaluation_status'] == 'no_issued_forecasts' for row in rows)
