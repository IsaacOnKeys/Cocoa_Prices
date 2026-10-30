"""Execute the changed forecast selection and gates; model arithmetic is unchanged."""

import re
import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PROCEDURE = (ROOT/'sql/procedures/run_analytics_release.sql').read_text()
CHECKS = (ROOT/'sql/quality/output_checks.sql').read_text()


def prediction(db, business='2026-10-30'):
    sql = re.search(r'CREATE TEMP TABLE candidate_forecast AS\s+(SELECT.*?)\'\'\'', PROCEDURE, re.S).group(1)
    # Substitute a deterministic model result while executing the actual input predicate.
    sql = re.sub(r'FROM ML.PREDICT\(MODEL `%s`,\(SELECT \* EXCEPT\(target_40\) FROM _SESSION.candidate_features\s+WHERE (.*?)\)\)',
                 r'FROM (SELECT * FROM candidate_features WHERE \1)', sql, flags=re.S)
    sql = sql.replace('DATE_ADD(date,INTERVAL 40 DAY)', "DATE(date,'+40 day')")
    sql = sql.replace('@business_date', ':business')
    return list(db.execute(sql, {'business': business}))


def violations(db, name, business='2026-10-30'):
    sql = CHECKS.split("UNION ALL SELECT 'output','"+name+"'", 1)[1].split('UNION ALL SELECT', 1)[0]
    sql = "SELECT 'output','"+name+"'"+sql
    sql = sql.replace('DATE_ADD(p_business_date,INTERVAL 40 DAY)', "DATE(:business,'+40 day')")
    sql = sql.replace('IS DISTINCT FROM', 'IS NOT')
    sql = re.sub(r'COUNTIF\((.*?)\),\n', r'COALESCE(SUM(\1),0),\n', sql, flags=re.S)
    return db.execute(sql, {'business': business}).fetchone()[3]


@pytest.fixture
def db():
    with sqlite3.connect(':memory:') as connection:
        connection.executescript('''
            CREATE TABLE candidate_features(date,target_40,predicted_target_40);
            CREATE TABLE candidate_forecast(forecast_date,forecast_cocoa_price);
            INSERT INTO candidate_features VALUES('2026-09-21',9999,1000);
            INSERT INTO candidate_features VALUES('2026-10-29',9999,2000);
            INSERT INTO candidate_features VALUES('2026-10-30',NULL,4500);
            INSERT INTO candidate_features VALUES('2026-10-31',NULL,6000);
        ''')
        yield connection


def test_only_business_date_features_produce_the_one_40_day_target(db):
    assert prediction(db) == [('2026-12-09',4500)]


def test_missing_current_features_cannot_fall_back_to_an_old_origin(db):
    db.execute("DELETE FROM candidate_features WHERE date='2026-10-30'")
    assert prediction(db) == []
    assert violations(db,'forecast_count') == 1


@pytest.mark.parametrize('count,expected', [(0,1),(1,0),(2,1),(40,39)])
def test_new_cardinality_gate_rejects_missing_duplicate_and_legacy_curves(db, count, expected):
    db.executemany('INSERT INTO candidate_forecast VALUES(?,?)', [('2026-12-09',4500)]*count)
    assert violations(db,'forecast_count') == expected


@pytest.mark.parametrize('target,expected', [('2026-12-09',0),('2026-12-08',1),
                                           ('2026-12-10',1),('2026-10-31',1),(None,1)])
def test_target_gate_requires_exactly_business_date_plus_40_days(db, target, expected):
    db.execute('INSERT INTO candidate_forecast VALUES(?,4500)',(target,))
    assert violations(db,'forecast_calendar') == expected


def test_calendar_horizon_crosses_year_boundary(db):
    db.execute("INSERT INTO candidate_features VALUES('2026-12-31',NULL,4700)")
    assert prediction(db,'2026-12-31') == [('2027-02-09',4700)]
