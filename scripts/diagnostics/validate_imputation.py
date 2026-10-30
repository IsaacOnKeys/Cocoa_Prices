"""Check exact bounded-fill SQL and BI provenance against fixed, sparse source facts."""

import argparse
import hashlib
import json
import re
import runpy
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROJECT = 'cocoa-prices-430315'
RENDER = runpy.run_path(str(ROOT/'scripts/deploy/render_delivery_sql.py'))['render']
FIXTURE = """
DECLARE p_business_date DATE DEFAULT DATE '2014-02-20';
CREATE TEMP TABLE candidate_cocoa AS
 SELECT DATE '2014-01-03' AS Date,100.0 AS Euro_Price UNION ALL
 SELECT DATE '2014-01-09',110.0 UNION ALL SELECT DATE '2014-02-06',120.0 UNION ALL
 SELECT DATE '2014-02-10',NULL UNION ALL SELECT DATE '2014-02-13',130.0;
CREATE TEMP TABLE candidate_oil AS
 SELECT DATE '2014-01-03' AS date,-1.0 AS brent_price_eu UNION ALL
 SELECT DATE '2014-01-09',0.0 UNION ALL SELECT DATE '2014-02-06',25.0;
CREATE TEMP TABLE candidate_weather AS
 SELECT DATE '2014-01-03' AS date,0.0 AS precipitation,0.1 AS soil_moisture UNION ALL
 SELECT DATE '2014-01-09',1.0,0.2 UNION ALL SELECT DATE '2014-01-12',NULL,0.3 UNION ALL
 SELECT DATE '2014-01-13',-999.0,-999.0 UNION ALL SELECT DATE '2014-02-06',2.0,0.4 UNION ALL
 SELECT DATE '2014-02-10',NULL,0.5;
"""

REVISIONS = """
CREATE TEMP TABLE source_input AS
 SELECT 'baseline' AS kind,'cocoa' AS feed,Date AS date,Euro_Price AS price,
   CAST(NULL AS FLOAT64) AS precipitation,CAST(NULL AS FLOAT64) AS soil_moisture FROM candidate_cocoa
 UNION ALL SELECT 'baseline','oil',date,brent_price_eu,NULL,NULL FROM candidate_oil
 UNION ALL SELECT 'baseline','weather',date,NULL,precipitation,soil_moisture FROM candidate_weather
 UNION ALL SELECT 'stage','cocoa',DATE '2014-01-09',NULL,NULL,NULL
 UNION ALL SELECT 'stage','oil',DATE '2014-01-09',NULL,NULL,NULL
 UNION ALL SELECT 'stage','weather',DATE '2014-01-09',NULL,NULL,0.25
 UNION ALL SELECT 'stage','weather',DATE '2014-01-14',NULL,NULL,NULL;
DROP TABLE candidate_cocoa;
DROP TABLE candidate_oil;
DROP TABLE candidate_weather;
"""


def names(token):
    if not re.fullmatch(r'[0-9]{8}_[a-f0-9]{8}', token):
        raise ValueError('Invalid imputation validation namespace')
    return 'imputation_validation_'+token


def checked(path):
    state = json.loads(path.read_text())
    if state['dataset'] != PROJECT+'.'+names(state['token']):
        raise ValueError('Only isolated imputation validation datasets are allowed')
    return state


def client():
    from google.cloud import bigquery
    from validate_ingestion import credentials

    return bigquery.Client(project=PROJECT, credentials=credentials(), location='europe-west3')


def exercise(path):
    from google.cloud import bigquery

    if path.exists():
        raise ValueError('Refusing to replace an imputation validation receipt')
    token = datetime.now(timezone.utc).strftime('%Y%m%d')+'_'+uuid.uuid4().hex[:8]
    state = {'token':token, 'dataset':PROJECT+'.'+names(token), 'phase':'preparing', 'jobs':{}}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2))
    bq = client()
    ds = bigquery.Dataset(state['dataset'])
    ds.location = 'europe-west3'
    ds.default_table_expiration_ms = 86400000
    ds.labels = {'app':'cocoa-prices', 'purpose':'imputation-validation', 'token':token}
    bq.create_dataset(ds, timeout=30)
    dataset = names(token)
    context = dict(delivery=dataset,curated=dataset+'_unused_curated',staging=dataset+'_unused_staging',
                   models=dataset,studio=dataset)

    def query(sql, name):
        job = bq.query(sql, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=1073741824,
            labels={'app':'cocoa-prices','purpose':'imputation-validation'}))
        state['jobs'][name] = job.job_id
        path.write_text(json.dumps(state, indent=2, default=str))
        return [dict(r) for r in job.result(timeout=120)]

    query(RENDER('migration', **context), 'migration')
    release = hashlib.sha256(token.encode()).hexdigest()
    prefix = state['dataset']
    artifacts = f"""
    INSERT `{prefix}.cocoa` SELECT '{release}',p_business_date,* FROM candidate_cocoa;
    INSERT `{prefix}.brent_prices` SELECT '{release}',p_business_date,* FROM candidate_oil;
    INSERT `{prefix}.precipitation` SELECT '{release}',p_business_date,* FROM candidate_weather;
    INSERT `{prefix}.joined_daily_cocoa_oil_weather` SELECT '{release}',p_business_date,* FROM candidate_joined;
    INSERT `{prefix}.frozen_inputs`
      (release_id,business_date,kind,feed,date,price,precipitation,soil_moisture,acquisition_id,observation_id)
      SELECT '{release}',p_business_date,kind,feed,date,price,precipitation,soil_moisture,
        IF(kind='stage',REPEAT('a',64),NULL),IF(kind='stage',REPEAT('b',64),NULL) FROM source_input;
    INSERT `{prefix}.release_runs`
      (release_id,business_date,dag_run_id,code_id,status,quality_results,created_at)
      VALUES('{release}',p_business_date,'synthetic_fill','{'0'*64}','published',
        JSON '{{"imputation_policy":{{"version":"bounded_calendar_v1"}}}}',CURRENT_TIMESTAMP());
    """
    sql = FIXTURE+REVISIONS+RENDER('revision_selection',**context)+RENDER('imputation_join', **context)+artifacts+f"""
    INSERT `{prefix}.active_release` VALUES(1,'{release}',p_business_date,CURRENT_TIMESTAMP());
    """
    query(sql, 'sparse_fill')
    query(RENDER('imputation_view', **context), 'compile_view')
    rows = query(f'SELECT * FROM `{prefix}.ls_series_quality` ORDER BY date,measurement', 'provenance')
    assert len(rows) == 50*4
    latest = [r for r in rows if r['is_latest']]
    assert len(latest)==4 and all(r['latest_measurement_overdue'] for r in latest)
    assert all(r['latest_measurement_age_days']>r['carry_limit_days'] for r in latest)
    assert all(r['latest_measurement_age_days'] is None and r['latest_measurement_overdue'] is None
               for r in rows if not r['is_latest'])
    lookup = {(str(r['date']),r['measurement']):r for r in rows}
    cases = [
        ('2014-01-02','cocoa_price',None,None,None,'unavailable',False,False),
        ('2014-01-03','cocoa_price',100.0,'2014-01-03',0,'observed',False,False),
        ('2014-01-04','cocoa_price',100.0,'2014-01-03',1,'carried',True,False),
        ('2014-01-16','cocoa_price',110.0,'2014-01-09',7,'carried',True,False),
        ('2014-01-17','cocoa_price',None,'2014-01-09',8,'expired',False,False),
        ('2014-01-04','oil_price',-1.0,'2014-01-03',1,'carried',True,False),
        ('2014-01-23','oil_price',0.0,'2014-01-09',14,'carried',True,False),
        ('2014-01-24','oil_price',None,'2014-01-09',15,'expired',False,False),
        ('2014-01-12','precipitation',1.0,'2014-01-09',3,'carried',True,True),
        ('2014-01-13','soil_moisture',0.3,'2014-01-12',1,'carried',True,True),
        ('2014-01-09','soil_moisture',0.25,'2014-01-09',0,'observed',False,False),
        ('2014-01-14','precipitation',1.0,'2014-01-09',5,'carried',True,True),
        ('2014-01-17','precipitation',None,'2014-01-09',8,'expired',False,False),
        ('2014-01-19','soil_moisture',0.3,'2014-01-12',7,'carried',True,False),
        ('2014-01-20','soil_moisture',None,'2014-01-12',8,'expired',False,False),
        ('2014-02-06','cocoa_price',120.0,'2014-02-06',0,'observed',False,False),
        ('2014-02-10','cocoa_price',120.0,'2014-02-06',4,'carried',True,True),
        ('2014-02-10','soil_moisture',0.5,'2014-02-10',0,'observed',False,False),
    ]
    for day,measurement,value,origin,age,status,imputed,missing in cases:
        row = lookup[day,measurement]
        assert row['published_value']==value and row['usable_value']==value, (day,measurement,row)
        assert (str(row['original_value_date']) if row['original_value_date'] else None)==origin
        assert (row['age_days'],row['quality_status'],row['is_imputed'],row['source_missing'])==(age,status,imputed,missing)
        assert row['policy_version']=='bounded_calendar_v1'
    preserved = [r for r in rows if str(r['date'])=='2014-01-09']
    assert len(preserved)==4 and sum(r['preserved_prior_value'] for r in preserved)==3
    assert all(r['latest_revision_missing'] and r['source_missing'] and r['latest_observation_id']=='b'*64
               for r in preserved if r['preserved_prior_value'])
    assert lookup['2014-01-14','precipitation']['preserved_prior_value'] is False
    # A corrected source fact keeps its observation date, not the correction time.
    revised = hashlib.sha256((token+'/revision').encode()).hexdigest()
    query(FIXTURE+REVISIONS.replace("SELECT 'stage','cocoa',DATE '2014-01-09',NULL", "SELECT 'stage','cocoa',DATE '2014-01-09',111.0")
          +RENDER('revision_selection',**context)+RENDER('imputation_join',**context)+artifacts.replace(release,revised)
          +f"UPDATE `{prefix}.active_release` SET release_id='{revised}' WHERE singleton=1;",'revised_source')
    revision = query(f"SELECT * FROM `{prefix}.ls_series_quality` WHERE measurement='cocoa_price' "
        "AND date IN (DATE '2014-01-09',DATE '2014-01-16',DATE '2014-01-17') ORDER BY date",'revision_report')
    assert [r['published_value'] for r in revision]==[111.0,111.0,None]
    assert [r['age_days'] for r in revision]==[0,7,8]
    assert all(str(r['original_value_date'])=='2014-01-09' and r['release_id']==revised for r in revision)
    retained = query(f"SELECT Euro_Price FROM `{prefix}.cocoa` WHERE release_id='{release}' AND Date=DATE '2014-01-09'",'retained_source')[0]
    assert retained['Euro_Price']==110.0
    # A legacy release may retain an old unlimited value; the BI field must blank it.
    query(f"UPDATE `{prefix}.active_release` SET release_id='{release}' WHERE singleton=1;"
          f"UPDATE `{prefix}.joined_daily_cocoa_oil_weather` SET cocoa_price=110 WHERE release_id='{release}' AND date=DATE '2014-01-17';"
          f"UPDATE `{prefix}.release_runs` SET quality_results=NULL WHERE release_id='{release}';", 'legacy_fixture')
    legacy = query(f"SELECT * FROM `{prefix}.ls_series_quality` WHERE date=DATE '2014-01-17' AND measurement='cocoa_price'", 'legacy_report')[0]
    assert legacy['published_value']==110 and legacy['usable_value'] is None and legacy['quality_status']=='expired'
    assert legacy['policy_version']=='legacy_unbounded_or_unknown' and legacy['age_days']==8
    state.update(phase='verified',boundary_cases=len(cases),legacy_expired_value_visible=True,
                 revision_origin_preserved=True,retained_source_unchanged=True,report_rows=rows,revision_report=revision)
    path.write_text(json.dumps(state,indent=2,default=str))
    print(json.dumps({'phase':'verified','boundary_cases':len(cases),'legacy_expiry_visible':True,
                      'revision_origin_preserved':True,'token':token}))


def cleanup(path):
    state = checked(path)
    bq = client()
    ds = bq.get_dataset(state['dataset'],timeout=30)
    if ds.labels.get('purpose')!='imputation-validation' or ds.labels.get('token')!=state['token']:
        raise ValueError('Refusing to delete an unlabelled imputation dataset')
    bq.delete_dataset(ds,delete_contents=True,timeout=60)
    state['resources_deleted'] = True
    path.write_text(json.dumps(state,indent=2))
    print(json.dumps({'resources_deleted':True,'token':state['token']}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase',choices=('exercise','cleanup'))
    parser.add_argument('--receipt',type=Path,required=True)
    args = parser.parse_args()
    {'exercise':exercise,'cleanup':cleanup}[args.phase](args.receipt)


if __name__=='__main__':
    main()
