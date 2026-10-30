"""Manual-only training, delayed labels, candidate gates and reviewed selection."""

import json
import math
import re
import runpy
import sqlite3
import sys
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
M = runpy.run_path(str(ROOT/'scripts/operations/manage_models.py'))
RENDER = runpy.run_path(str(ROOT/'scripts/deploy/render_delivery_sql.py'))['render']


def request():
    return M['scope']('a'*64,date(2020,6,1),date(2020,1,1),'review-example')


def metrics(**changes):
    return {'holdout_rows':20,'invalid_predictions':0,'model_mae':100.0,
            'persistence_mae':100.0,'seasonal_mae':120.0,**changes}


def test_candidate_identity_is_stable_and_cannot_overwrite_the_accepted_model():
    assert request() == request()
    assert '.cocoa_candidate_' in request()['model_id']
    assert 'CREATE MODEL `' in M['train_sql'](request()['model_id'])
    with pytest.raises(ValueError):
        M['train_sql']('cocoa-prices-430315.models.cocoa_release_existing')


def test_training_selects_only_labels_known_with_the_one_day_publication_buffer():
    sql = M['train_sql'](request()['model_id'])
    predicate = sql.split('WHERE ',1)[1]
    predicate = predicate.replace('DATE_ADD(date,INTERVAL 41 DAY)',"DATE(date,'+41 day')")
    for parameter in ('source','business','cutoff'):
        predicate = predicate.replace('@'+parameter,':'+parameter)
    with sqlite3.connect(':memory:') as db:
        db.executescript('''CREATE TABLE features(release_id,business_date,date,target_40);
          INSERT INTO features VALUES('source','2020-06-01','2019-11-21',100);
          INSERT INTO features VALUES('source','2020-06-01','2019-11-22',110);
          INSERT INTO features VALUES('source','2020-06-01','2019-11-20',NULL);''')
        rows = list(db.execute('SELECT date FROM features WHERE '+predicate,
                              {'source':'source','business':'2020-06-01','cutoff':'2020-01-01'}))
    assert rows == [('2019-11-21',)]
    assert "data_split_method='NO_SPLIT'" in sql


@pytest.mark.parametrize('training,changes', [
    (364,{}),(365,{'holdout_rows':19}),(365,{'invalid_predictions':1}),
    (365,{'model_mae':math.nan}),(365,{'model_mae':math.inf}),
    (365,{'model_mae':-1}),(365,{'model_mae':105.01}),
    (365,{'model_mae':100,'seasonal_mae':50}),
])
def test_invalid_or_materially_degraded_candidates_are_blocked(training, changes):
    with pytest.raises(ValueError):
        M['review_gate'](training,metrics(**changes))


def test_boundary_and_perfect_baseline_have_defined_gates():
    M['review_gate'](365,metrics(model_mae=105))
    M['review_gate'](365,metrics(model_mae=0,persistence_mae=0))
    with pytest.raises(ValueError):
        M['review_gate'](365,metrics(model_mae=0.01,persistence_mae=0))


def test_model_schema_and_evaluated_version_are_checked():
    model = SimpleNamespace(model_type='LINEAR_REGRESSION',etag='reviewed',
                            feature_columns=[SimpleNamespace(name=name) for name in M['FEATURES']],
                            label_columns=[SimpleNamespace(name='predicted_target_40')])
    M['verify_model'](model,'reviewed')
    with pytest.raises(ValueError):
        M['verify_model'](model,'different')
    model.feature_columns.pop()
    with pytest.raises(ValueError):
        M['verify_model'](model)


def test_preview_starts_no_cloud_work_and_writes_no_receipt(monkeypatch,tmp_path,capsys):
    output=tmp_path/'candidate.json'
    monkeypatch.setattr(sys,'argv',['manage_models.py','train','--source-release','a'*64,
        '--business-date','2020-06-01','--cutoff','2020-01-01','--key','preview','--output',str(output)])
    monkeypatch.setitem(sys.modules,'google.cloud.bigquery',None)
    M['main']()
    assert json.loads(capsys.readouterr().out)['executed'] is False
    assert not output.exists()


def test_daily_release_contains_no_training_and_freezes_the_reviewed_choice():
    sql = RENDER('procedure')
    assert not re.search(r'\bCREATE\s+(OR\s+REPLACE\s+)?MODEL\b',sql,re.I)
    assert 'model_policy` WHERE singleton=1' in sql
    assert 'model_id,model_card,created_at)' in sql
    assert 'SELECT AS STRUCT model_id,model_card\n      FROM `cocoa-prices-430315.analytics_delivery.release_runs`' in sql
    assert 'ML.PREDICT' in sql


def test_stale_review_cannot_change_model_selection():
    sql = RENDER('model_selection')
    update = re.search(r'(UPDATE `[^`]+`.*?WHERE singleton=1 AND review_id=p_expected);',sql,re.S).group(1)
    update = re.sub(r'`[^`]+`','model_policy',update).replace('CURRENT_TIMESTAMP()','CURRENT_TIMESTAMP')
    update = update.replace('=card', '=:card')
    for name in ('model','review','expected'):
        update = update.replace('p_'+name,':'+name)
    with sqlite3.connect(':memory:') as db:
        db.executescript("CREATE TABLE model_policy(singleton,model_id,review_id,model_card,updated_at); INSERT INTO model_policy VALUES(1,'old','review0','{}',NULL);")
        params={'model':'new','review':'review1','expected':'review0','card':'{}'}
        assert db.execute(update,params).rowcount == 1
        assert db.execute(update,{**params,'model':'stale'}).rowcount == 0
        assert db.execute('SELECT model_id FROM model_policy').fetchone()[0] == 'new'
    assert "ASSERT @@row_count=1" in sql and 'ROLLBACK TRANSACTION' in sql


@pytest.mark.parametrize('count,status', [(0,'provisional_insufficient_matured_forecasts'),
                                         (19,'provisional_insufficient_matured_forecasts'),
                                         (20,'review_required_baseline_degradation')])
def test_live_model_health_does_not_call_unknown_accuracy_good(count,status):
    sql = RENDER('model_health_view').split(' AS\n',1)[1]
    sql = sql.replace('`cocoa-prices-430315.cocoa_related_studio.','`').replace('`cocoa-prices-430315.analytics_delivery.','`')
    sql = sql.replace("IF(evaluation_status='evaluated',","IIF(evaluation_status='evaluated',").replace('LEAST(','MIN(')
    sql = sql.replace("COUNTIF(evaluation_status='evaluated')","SUM(CASE WHEN evaluation_status='evaluated' THEN 1 ELSE 0 END)")
    with sqlite3.connect(':memory:') as db:
        db.row_factory=sqlite3.Row
        db.executescript('''CREATE TABLE issued_forecast_evaluation(model_id,evaluation_status,forecast_cocoa_price,actual_observed_price,persistence_price,weekly_seasonal_price);
          CREATE TABLE release_runs(release_id,model_id,model_card);
          CREATE TABLE active_release(singleton,release_id);
          CREATE TABLE model_policy(singleton,model_id,review_id,updated_at,model_card);
          INSERT INTO release_runs VALUES('release','accepted','{}');
          INSERT INTO active_release VALUES(1,'release');
          INSERT INTO model_policy VALUES(1,'selected','review','time','{}');''')
        db.executemany("INSERT INTO issued_forecast_evaluation VALUES('accepted','evaluated',120,100,100,101)",[()]*count)
        row=dict(db.execute(sql).fetchone())
    assert row['performance_status'] == status
    assert row['serving_model_id'] == 'accepted' and row['selected_model_for_new_releases'] == 'selected'
