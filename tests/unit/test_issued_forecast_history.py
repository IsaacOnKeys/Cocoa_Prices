"""Retained issue identity, revision history and honest retrospective labels."""

import runpy
import sqlite3
from datetime import date
from pathlib import Path

import pendulum
import pytest

ROOT = Path(__file__).resolve().parents[2]
RENDER = runpy.run_path(str(ROOT/'scripts/deploy/render_delivery_sql.py'))['render']


@pytest.fixture
def history():
    db = sqlite3.connect(':memory:')
    db.row_factory = sqlite3.Row
    db.create_function('HORIZON', 2, lambda target, issued:
                       (date.fromisoformat(target)-pendulum.parse(issued).in_timezone('Europe/Berlin').date()).days)
    db.executescript('''
        CREATE TABLE release_runs(release_id,business_date,status,published_at,input_cutoff,model_id,code_id);
        CREATE TABLE forecast_40d(release_id,business_date,forecast_date,forecast_cocoa_price);
        CREATE TABLE pred_hist_40d(release_id,business_date,prediction_date,forecast_date,yhat_40,actual);
        CREATE TABLE frozen_inputs(release_id,business_date,kind,feed,date,price,acquisition_id);
        INSERT INTO release_runs VALUES('old','2026-10-23','published','2026-10-23T17:15:00Z','2026-10-23T17:05:00Z','old_model','old_code');
        INSERT INTO forecast_40d VALUES('old','2026-10-23','2026-11-30',4500.0);
        INSERT INTO pred_hist_40d VALUES('old','2026-10-23','2026-08-01','2026-09-10',4000.0,4050.0);
        INSERT INTO frozen_inputs VALUES('old','2026-10-23','baseline','cocoa','2026-10-22',4400.0,NULL);
        INSERT INTO frozen_inputs VALUES('old','2026-10-23','stage','cocoa','2026-10-22',NULL,'missing-revision');
    ''')
    sql = RENDER('forecast_history_views')
    sql = sql.replace('CREATE OR REPLACE VIEW', 'CREATE VIEW')
    sql = sql.replace('`cocoa-prices-430315.cocoa_related_studio.', '`')
    sql = sql.replace('`cocoa-prices-430315.analytics_delivery.', '`')
    sql = sql.replace("DATE_DIFF(f.forecast_date,DATE(r.published_at,'Europe/Berlin'),DAY)",
                      'HORIZON(f.forecast_date,r.published_at)')
    db.executescript(sql)
    yield db
    db.close()


def test_new_release_cannot_rewrite_an_earlier_issue(history):
    before = dict(history.execute("SELECT * FROM issued_forecasts WHERE release_id='old'").fetchone())
    history.executescript('''
        INSERT INTO release_runs VALUES('new','2026-10-26','published','2026-10-26T18:15:00Z','2026-10-26T18:05:00Z','new_model','new_code');
        INSERT INTO forecast_40d VALUES('new','2026-10-26','2026-11-30',4800.0);
    ''')
    assert dict(history.execute("SELECT * FROM issued_forecasts WHERE release_id='old'").fetchone()) == before
    assert history.execute('SELECT COUNT(*) FROM issued_forecasts').fetchone()[0] == 2
    assert before['model_id'] == 'old_model' and before['transformation_sha256'] == 'old_code'
    assert before['issued_at'] == '2026-10-23T17:15:00Z'
    assert before['feature_cutoff'] == '2026-10-23T17:05:00Z'
    assert before['prediction_kind'] == 'issued_snapshot' and before['unit'] == 'EUR/metric tonne'


@pytest.mark.parametrize('status,published', [('failed','2026-10-24T17:00:00Z'),
                                             ('validated',None), ('published',None),
                                             ('legacy_verified',None)])
def test_unissued_candidates_and_unknown_legacy_times_are_excluded(history, status, published):
    history.execute('UPDATE release_runs SET status=?,published_at=?', (status,published))
    for view in ('issued_forecasts','retrospective_scores','source_observation_vintages'):
        assert history.execute('SELECT COUNT(*) FROM '+view).fetchone()[0] == 0


@pytest.mark.parametrize('issued,target,horizon', [
    ('2026-10-24T23:30:00Z','2026-10-26',1),  # Berlin is already Sunday on the DST weekend.
    ('2026-10-25T23:30:00Z','2026-10-26',0),
    ('2026-10-27T18:00:00Z','2026-10-26',-1),  # Late issuance is not backdated.
])
def test_horizon_uses_actual_berlin_issue_date(history, issued, target, horizon):
    history.execute('UPDATE release_runs SET published_at=?', (issued,))
    history.execute('UPDATE forecast_40d SET forecast_date=?', (target,))
    assert history.execute('SELECT horizon_days_from_issue FROM issued_forecasts').fetchone()[0] == horizon


def test_historical_rescores_are_not_presented_as_out_of_sample(history):
    row = dict(history.execute('SELECT * FROM retrospective_scores').fetchone())
    assert row['prediction_kind'] == 'retrospective_rescore'
    assert not row['is_chronological_out_of_sample']
    assert row['feature_origin_date'] == '2026-08-01' and row['target_date'] == '2026-09-10'
    assert row['actual'] == 4050.0


def test_vintages_preserve_baseline_and_incoming_missing_revision(history):
    rows = [dict(row) for row in history.execute('SELECT * FROM source_observation_vintages ORDER BY kind')]
    assert len(rows) == 2
    assert rows[0]['price'] == 4400.0 and rows[0]['acquisition_id'] is None
    assert rows[1]['price'] is None and rows[1]['acquisition_id'] == 'missing-revision'
    assert rows[0]['input_cutoff'] == rows[1]['input_cutoff'] == '2026-10-23T17:05:00Z'


def test_history_renderer_supports_an_isolated_namespace():
    sql = RENDER('forecast_history_views', project='unit-project', delivery='isolated_delivery', studio='isolated_bi')
    assert 'cocoa-prices-430315' not in sql and '${' not in sql
    assert '`unit-project.isolated_delivery.forecast_40d`' in sql
