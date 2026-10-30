"""Validate frozen inputs and atomic publication in disposable BigQuery namespaces."""

import argparse
import contextlib
import hashlib
import io
import json
import re
import runpy
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

PROJECT = 'cocoa-prices-430315'
LOCATION = 'europe-west3'
ROOT = Path(__file__).resolve().parents[2]
RENDER = runpy.run_path(str(ROOT / 'scripts/deploy/render_delivery_sql.py'))['render']
SERVING = ('cocoa', 'brent_prices', 'precipitation', 'joined_daily_cocoa_oil_weather', 'forecast_40d', 'pred_hist_40d')
RECEIPT = None


def save(state):
    if RECEIPT is not None:
        RECEIPT.write_text(json.dumps(state, indent=2, default=str))


def names(token):
    if not re.fullmatch(r'20[0-9]{6}_[a-f0-9]{8}', token):
        raise ValueError('Invalid atomic validation namespace')
    base = 'atomic_validation_' + token
    return {'delivery': base, 'curated': base+'_serving', 'staging': base+'_staging', 'models': base, 'studio': base+'_serving'}


def checked(path):
    state = json.loads(path.read_text())
    if state['names'] != names(state['token']):
        raise ValueError('Only isolated atomic validation datasets are allowed')
    return state


def client():
    from google.cloud import bigquery
    from validate_ingestion import credentials

    return bigquery.Client(project=PROJECT, credentials=credentials(), location=LOCATION)


def query(bq, state, sql, *, params=(), name, timeout=180):
    from google.cloud import bigquery

    config = bigquery.QueryJobConfig(query_parameters=list(params), maximum_bytes_billed=1073741824,
                                    labels={'app': 'cocoa-prices', 'purpose': 'atomic-validation', 'token': state['token']})
    job = bq.query(sql, job_config=config, location=LOCATION)
    state.setdefault('jobs', {})[name] = job.job_id
    save(state)
    try:
        return [dict(row) for row in job.result(timeout=timeout)]
    except Exception:
        state.setdefault('failed_jobs', {})[name] = job.job_id
        save(state)
        raise


def fingerprints(bq, state):
    dataset = state['names']['curated']
    sql = ' UNION ALL '.join(f"SELECT '{table}' name,COUNT(*) row_count,COALESCE(CAST(BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS STRING),'0') fingerprint FROM `{PROJECT}.{dataset}.{table}` t" for table in SERVING)
    return sorted(query(bq, state, sql, name='fingerprints_'+uuid.uuid4().hex[:8]), key=lambda r: r['name'])


def source_manifest(bq, state, feed, table, acquisition):
    from google.cloud import bigquery

    measures={'cocoa':('cocoa_price',),'oil':('oil_price',),'weather':('precipitation','soil_moisture')}[feed]
    nulls=','.join(f'{name} IS NULL AS {name}_missing' for name in measures)
    rows=query(bq,state,f'SELECT DISTINCT date,observation_id,{nulls} FROM `{PROJECT}.{state["names"]["staging"]}.{table}` WHERE acquisition_id=@acquisition ORDER BY date',
      params=[bigquery.ScalarQueryParameter('acquisition','STRING',acquisition)],name='source_manifest_'+feed)
    if len(rows)>90:
        raise ValueError('Synthetic source inventory exceeds the supported window')
    return [{'date':str(row['date']),'observation_id':row['observation_id'],
             'missing_measurements':[name for name in measures if row[name+'_missing']]} for row in rows]


def prepare(path, historical_gaps=False, revision_cases=False):
    from google.cloud import bigquery

    if path.exists():
        raise ValueError('Refusing to replace an atomic validation receipt')
    token = datetime.now(timezone.utc).strftime('%Y%m%d')+'_'+uuid.uuid4().hex[:8]
    fixture_time=datetime.now(timezone.utc)-timedelta(seconds=1)
    state = {'token': token, 'names': names(token), 'phase': 'preparing',
             'business_date': fixture_time.date().isoformat(),'fixture_acquired_at':fixture_time.isoformat(),
             'historical_gaps':historical_gaps,'revision_cases':revision_cases}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2))
    bq = client()
    for name in sorted(set(state['names'].values())):
        dataset = bigquery.Dataset(PROJECT+'.'+name)
        dataset.location = LOCATION
        dataset.default_table_expiration_ms = 86400000
        dataset.labels = {'app': 'cocoa-prices', 'purpose': 'atomic-validation', 'token': token}
        bq.create_dataset(dataset, timeout=30)
    context = state['names']
    curated, staging = context['curated'], context['staging']
    script = f'''
      CREATE TABLE `{PROJECT}.{curated}.cocoa` AS
        SELECT d AS Date,6000.0+offset*0.2+10*SIN(offset/5.0) AS Euro_Price
        FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2014-01-02',DATE_SUB(DATE '{state["business_date"]}',INTERVAL 1 DAY))) d WITH OFFSET;
      CREATE TABLE `{PROJECT}.{curated}.brent_prices` AS
        SELECT Date AS date,75.0+SIN(UNIX_DATE(Date)/5.0) AS brent_price_eu FROM `{PROJECT}.{curated}.cocoa`;
      CREATE TABLE `{PROJECT}.{curated}.precipitation` AS
        SELECT Date AS date,3.0+0.2*COS(UNIX_DATE(Date)/7.0) AS precipitation,0.4+0.01*SIN(UNIX_DATE(Date)/4.0) AS soil_moisture FROM `{PROJECT}.{curated}.cocoa`;
      CREATE TABLE `{PROJECT}.{curated}.joined_daily_cocoa_oil_weather` AS
        SELECT c.Date AS date,c.Euro_Price AS cocoa_price,o.brent_price_eu AS oil_price,w.precipitation,w.soil_moisture
        FROM `{PROJECT}.{curated}.cocoa` c JOIN `{PROJECT}.{curated}.brent_prices` o ON c.Date=o.date
        JOIN `{PROJECT}.{curated}.precipitation` w ON c.Date=w.date;
      CREATE TABLE `{PROJECT}.{curated}.forecast_40d` AS
        SELECT DATE_ADD(DATE '{state["business_date"]}',INTERVAL offset DAY) AS forecast_date,6900.0+offset AS forecast_cocoa_price FROM UNNEST(GENERATE_ARRAY(0,39)) offset;
      CREATE TABLE `{PROJECT}.{curated}.pred_hist_40d` AS
        SELECT c.Date AS prediction_date,DATE_ADD(c.Date,INTERVAL 40 DAY) AS forecast_date,c.Euro_Price+8 AS yhat_40,f.Euro_Price AS actual
        FROM `{PROJECT}.{curated}.cocoa` c LEFT JOIN `{PROJECT}.{curated}.cocoa` f ON f.Date=DATE_ADD(c.Date,INTERVAL 40 DAY);
    '''
    if historical_gaps:
        marker = f'CREATE TABLE `{PROJECT}.{curated}.joined_daily_cocoa_oil_weather` AS'
        faults = f'''DELETE `{PROJECT}.{curated}.cocoa` WHERE Date BETWEEN DATE '2014-01-10' AND DATE '2014-02-05';
          DELETE `{PROJECT}.{curated}.brent_prices` WHERE date BETWEEN DATE '2014-01-10' AND DATE '2014-02-25';
          UPDATE `{PROJECT}.{curated}.precipitation` SET precipitation=NULL
            WHERE date BETWEEN DATE '2014-01-10' AND DATE '2014-02-05';
          UPDATE `{PROJECT}.{curated}.precipitation` SET soil_moisture=-999.0
            WHERE date BETWEEN DATE '2014-01-13' AND DATE '2014-02-05';
        '''
        assert script.count(marker)==1
        script=script.replace(marker,faults+marker)
    if revision_cases:
        marker = f'CREATE TABLE `{PROJECT}.{curated}.joined_daily_cocoa_oil_weather` AS'
        duplicates = ''.join(f"INSERT `{PROJECT}.{curated}.{table}` SELECT * FROM `{PROJECT}.{curated}.{table}` WHERE {date_column}=DATE_SUB(DATE '{state['business_date']}',INTERVAL 2 DAY);"
                             for table,date_column in [('cocoa','Date'),('brent_prices','date'),('precipitation','date')])
        assert script.count(marker)==1
        script=script.replace(marker,duplicates+marker)
    query(bq, state, script, name='seed_serving')
    for feed, table, fields in [('cocoa', 'cocoa_prices', [('cocoa_price','FLOAT64')]),
                                ('oil', 'oil_prices', [('oil_price','FLOAT64')]),
                                ('weather', 'precipitation_moisture', [('precipitation','FLOAT64'),('soil_moisture','FLOAT64')])]:
        columns = [('date','DATE'), *fields, ('ingestion_time','TIMESTAMP'), ('raw_payload','STRING'),
                   ('producer_acquired_at','TIMESTAMP'), ('producer_timestamp_raw','STRING'),
                   ('consumer_received_at','TIMESTAMP'), ('source_published_at','TIMESTAMP'),
                   ('timestamp_status','STRING'), ('delivery_id','STRING'), ('pubsub_message_id','STRING'),
                   ('acquisition_id','STRING'), ('observation_id','STRING')]
        bq.create_table(bigquery.Table(f'{PROJECT}.{staging}.{table}', schema=[bigquery.SchemaField(k,t) for k,t in columns]), timeout=30)
    for name in ['features_live', 'mape_40d']:
        text = bq.get_table(f'{PROJECT}.cocoa_related.{name}',timeout=30).view_query
        text = text.replace(PROJECT+'.cocoa_related.', PROJECT+'.'+curated+'.')
        if PROJECT+'.cocoa_related.' in text or PROJECT+'.stream_staging.' in text:
            raise ValueError('Unmapped live reference in the synthetic view fixture')
        query(bq, state, f'CREATE VIEW `{PROJECT}.{curated}.{name}` AS '+text, name='seed_'+name)
    query(bq, state, RENDER('migration', **context), name='migration')
    path.write_text(json.dumps(state, indent=2, default=str))
    query(bq, state, RENDER('procedure', **context), name='compile_procedure')
    baseline = fingerprints(bq, state)
    state['baseline'] = baseline
    seed = hashlib.sha256(('verified-fixture-'+token).encode()).hexdigest()
    state['seed_release_id'] = seed
    params = [bigquery.ScalarQueryParameter('release_id','STRING',seed),
              bigquery.ScalarQueryParameter('business_date','DATE',date.fromisoformat(state['business_date'])-timedelta(days=1)),
              bigquery.ScalarQueryParameter('code_id','STRING','0'*64),
              bigquery.ScalarQueryParameter('fingerprints','JSON',baseline),
              bigquery.ScalarQueryParameter('model_id','STRING','fixture_legacy_model'),
              bigquery.ScalarQueryParameter('evidence','JSON',{'classification':'synthetic_validation_fixture'})]
    query(bq, state, RENDER('bootstrap', **context), params=params, name='bootstrap')
    assert fingerprints(bq, state) == baseline
    state['phase'] = 'ready'
    path.write_text(json.dumps(state, indent=2, default=str))
    print(json.dumps({'phase': 'ready', 'token': token, 'isolated_datasets': sorted(set(context.values()))}), flush=True)


def cleanup(path):
    from google.api_core.exceptions import NotFound

    state = checked(path)
    bq = client()
    for name in sorted(set(state['names'].values())):
        try:
            dataset = bq.get_dataset(PROJECT+'.'+name, timeout=30)
        except NotFound:
            continue
        if dataset.labels.get('purpose') != 'atomic-validation' or dataset.labels.get('token') != state['token']:
            raise ValueError('Refusing to delete an unlabelled atomic validation dataset')
        bq.delete_dataset(dataset, delete_contents=True, timeout=60)
    state['resources_deleted'] = True
    path.write_text(json.dumps(state, indent=2, default=str))
    print(json.dumps({'resources_deleted': True, 'token': state['token']}))


def exercise(path, inject_failures=True):
    from google.api_core.exceptions import BadRequest
    from google.cloud import bigquery

    state = checked(path)
    if state['phase'] != 'ready':
        raise ValueError('Do not automatically resume an uncertain publication exercise')
    bq = client()
    context = state['names']
    query(bq, state, RENDER('procedure', **context), name='compile_current_procedure')
    routine = bq.get_routine(f'{PROJECT}.{context["delivery"]}.run_daily_pipeline', timeout=30)
    code_id = hashlib.sha256(routine.body.encode()).hexdigest()
    release = hashlib.sha256((state['token']+'/'+code_id+'/first').encode()).hexdigest()
    acquired = datetime.fromisoformat(state['fixture_acquired_at'])
    started = acquired-timedelta(seconds=1)
    sources = {}
    for feed, table, expression, fields in [
        ('cocoa','cocoa_prices',"6000.0+DATE_DIFF(d,DATE '2014-01-02',DAY)*0.2+10*SIN(DATE_DIFF(d,DATE '2014-01-02',DAY)/5.0)+5",'cocoa_price'),
        ('oil','oil_prices','75.0+SIN(UNIX_DATE(d)/5.0)+0.01','oil_price'),
        ('weather','precipitation_moisture','3.0+0.2*COS(UNIX_DATE(d)/7.0),0.4+0.01*SIN(UNIX_DATE(d)/4.0)','precipitation,soil_moisture')]:
        identity = hashlib.sha256((release+'/'+feed).encode()).hexdigest()
        sources[feed] = {'acquisition_id': identity, 'acquired_at': acquired.isoformat(), 'observations': 31,
                         'receipt_object': 'synthetic-publication-fixture/'+feed, 'receipt_generation': '1',
                         'receipt_sha256': 'c'*64, 'engineering_validation': True}
        params = [bigquery.ScalarQueryParameter('acquisition','STRING',identity),
                  bigquery.ScalarQueryParameter('acquired','TIMESTAMP',acquired),
                  bigquery.ScalarQueryParameter('business','DATE',date.fromisoformat(state['business_date']))]
        sql = f'''INSERT `{PROJECT}.{context["staging"]}.{table}`
          (date,{fields},ingestion_time,raw_payload,producer_acquired_at,producer_timestamp_raw,
           consumer_received_at,source_published_at,timestamp_status,delivery_id,pubsub_message_id,acquisition_id,observation_id)
          SELECT d,{expression},@acquired,'synthetic-publication-fixture',@acquired,CAST(@acquired AS STRING),
            TIMESTAMP_ADD(@acquired,INTERVAL 2 SECOND),CAST(NULL AS TIMESTAMP),'valid',
            TO_HEX(SHA256(CAST(d AS STRING))),CAST(d AS STRING),@acquisition,
            LOWER(TO_HEX(SHA256(CONCAT(@acquisition,CAST(d AS STRING)))))
          FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(@business,INTERVAL 30 DAY),@business)) d'''
        query(bq, state, sql, params=params, name='stage_'+feed)
        if state.get('revision_cases'):
            missing = 'precipitation=NULL' if feed=='weather' else fields+'=NULL'
            query(bq,state,f'UPDATE `{PROJECT}.{context["staging"]}.{table}` SET {missing} WHERE date=DATE_SUB(@business,INTERVAL 1 DAY);'
                f'INSERT `{PROJECT}.{context["staging"]}.{table}` SELECT * REPLACE(TIMESTAMP_ADD(consumer_received_at,INTERVAL 1 SECOND) AS consumer_received_at) '
                f'FROM `{PROJECT}.{context["staging"]}.{table}` WHERE date=DATE_SUB(@business,INTERVAL 2 DAY);',params=params,name='revision_cases_'+feed)
        sources[feed].update(window_start=(date.fromisoformat(state['business_date'])-timedelta(days=30)).isoformat(),
                             window_end=state['business_date'],observation_manifest=source_manifest(bq,state,feed,table,identity))
    canonical_sources = json.dumps(sources, sort_keys=True, separators=(',', ':'))
    state.update(phase='exercising', release_id=release, code_id=code_id, source_receipts=sources,
                 run_started_at=started.isoformat())
    save(state)

    def invoke(stage=None, *, alternate_sources=None, name):
        params = [bigquery.ScalarQueryParameter('release','STRING',release),
                  bigquery.ScalarQueryParameter('business','DATE',date.fromisoformat(state['business_date'])),
                  bigquery.ScalarQueryParameter('run','STRING','atomic_fixture_'+state['token']),
                  bigquery.ScalarQueryParameter('started','TIMESTAMP',started),
                  bigquery.ScalarQueryParameter('sources','STRING',canonical_sources if alternate_sources is None else alternate_sources),
                  bigquery.ScalarQueryParameter('code','STRING',code_id),
                  bigquery.ScalarQueryParameter('stage','STRING',stage)]
        return query(bq, state, f'CALL `{PROJECT}.{context["delivery"]}.run_daily_pipeline`(@release,@business,@run,@started,@sources,@code,@stage)',
                     params=params, name=name, timeout=240)

    def fail(stage):
        try:
            invoke(stage, name='inject_'+stage)
        except BadRequest as error:
            if 'Injected failure' not in str(error):
                raise
        else:
            raise AssertionError('Injected failure unexpectedly succeeded')
        assert fingerprints(bq, state) == state['baseline']
        active = query(bq, state, f'SELECT release_id FROM `{PROJECT}.{context["delivery"]}.active_release`', name='marker_'+stage)
        assert active == [{'release_id': state['seed_release_id']}]
        state.setdefault('injected_failures_verified', []).append(stage)
        save(state)
        print(json.dumps({'injected_failure': stage, 'six_serving_fingerprints_unchanged': True}), flush=True)

    if inject_failures:
        fail('after_freeze')
    identity = sources['cocoa']['acquisition_id']
    sql = f'''INSERT `{PROJECT}.{context["staging"]}.cocoa_prices`
      SELECT date,cocoa_price+123,ingestion_time,raw_payload,producer_acquired_at,producer_timestamp_raw,
        CURRENT_TIMESTAMP(),source_published_at,timestamp_status,'late-fixture',pubsub_message_id,acquisition_id,
        TO_HEX(SHA256(CONCAT(observation_id,'late')))
      FROM `{PROJECT}.{context["staging"]}.cocoa_prices`
      WHERE acquisition_id=@acquisition AND date=@business LIMIT 1'''
    if inject_failures:
        query(bq, state, sql, params=[bigquery.ScalarQueryParameter('acquisition','STRING',identity),
              bigquery.ScalarQueryParameter('business','DATE',date.fromisoformat(state['business_date']))], name='late_conflicting_staging')
        fail('after_candidates')
        fail('after_first_publish')
    result = invoke(name='publish')
    assert len(result) == 1 and result[0]['status'] == 'published' and result[0]['is_active']
    state['published_result'] = result[0]
    quality=query(bq,state,f'SELECT quality_results FROM `{PROJECT}.{context["delivery"]}.release_runs` WHERE release_id=@release',
      params=[bigquery.ScalarQueryParameter('release','STRING',release)],name='published_quality_evidence')[0]['quality_results']
    assert quality['checks'] and all(check['violations']==0 for check in quality['checks'] if check['severity']=='error')
    assert quality['revision_policy']['null_policy']=='preserve_known_value'
    if state.get('revision_cases'):
        assert quality['preserved_missing_measurements']==3
    state['published_quality_report']=quality
    published = fingerprints(bq, state)
    assert published != state['baseline']
    model = bq.get_model(result[0]['model_id'], timeout=30)
    state['model_etag'] = model.etag
    original = query(bq, state, f'''SELECT c.Euro_Price=f.price AS frozen_value_published
      FROM `{PROJECT}.{context["curated"]}.cocoa` c JOIN `{PROJECT}.{context["delivery"]}.frozen_inputs` f ON c.Date=f.date
      WHERE f.release_id=@release AND f.kind='stage' AND f.feed='cocoa' AND f.date=@business''',
      params=[bigquery.ScalarQueryParameter('release','STRING',release),bigquery.ScalarQueryParameter('business','DATE',date.fromisoformat(state['business_date']))], name='verify_frozen_value')
    assert original == [{'frozen_value_published': True}]
    assert invoke(name='published_retry') == result
    assert fingerprints(bq, state) == published and bq.get_model(result[0]['model_id'], timeout=30).etag == model.etag
    try:
        invoke(alternate_sources=canonical_sources+' ', name='reject_changed_frozen_request')
    except BadRequest as error:
        if 'Retry scope/code/source receipts differ' not in str(error):
            raise
    else:
        raise AssertionError('Changed frozen request unexpectedly succeeded')
    assert fingerprints(bq, state) == published
    state.update(phase='publication_verified', published_fingerprints=published, late_staging_ignored_on_retry=inject_failures,
                 completed_retry_is_noop=True, changed_request_rejected=True)
    save(state)
    print(json.dumps({'phase': 'publication_verified', 'token': state['token'], 'failure_points': 3 if inject_failures else 0,
                      'frozen_input_retry': inject_failures, 'completed_retry_noop': True}), flush=True)


def rollback(path):
    from google.api_core.exceptions import BadRequest
    from google.cloud import bigquery

    state = checked(path)
    if state['phase'] != 'publication_verified':
        raise ValueError('Rollback validation requires the verified isolated publication')
    bq = client()
    n = state['names']
    query(bq,state,RENDER('migration',**n),name='migration_recovery_events')
    query(bq,state,RENDER('rollback',**n),name='compile_rollback')
    query(bq,state,RENDER('serving_views',**n),name='compile_serving_status')
    query(bq,state,RENDER('imputation_view',**n),name='compile_imputation_status')
    cli = runpy.run_path(str(ROOT/'scripts/operations/rollback_release.py'))

    def invoke(target, current=None, key=None, *, apply=False):
        args = ['rollback_release.py',target,'--delivery',n['delivery']]
        if apply:
            args += ['--apply','--expected-current',current,'--incident-key',key,'--reason','isolated recovery proof']
        output = io.StringIO()
        with patch('google.cloud.bigquery.Client',return_value=bq),patch.object(sys,'argv',args),contextlib.redirect_stdout(output):
            cli['main']()
        return json.loads(output.getvalue())

    current = state['release_id']
    seed = state['seed_release_id']

    def imputation_report(expected,policy,stage):
        rows=query(bq,state,f'''SELECT release_id,policy_version,COUNT(*) AS row_count,
          COUNTIF(quality_status='expired') AS expired_rows,
          COUNTIF(quality_status='inconsistent') AS inconsistent_rows,
          COUNTIF(quality_status='expired' AND published_value IS NOT NULL) AS expired_present
          FROM `{PROJECT}.{n["studio"]}.ls_series_quality` GROUP BY release_id,policy_version''',name='imputation_'+stage)
        assert len(rows)==1 and rows[0]['release_id']==expected and rows[0]['policy_version']==policy
        assert rows[0]['inconsistent_rows']==0
        if policy=='bounded_calendar_v1':
            assert rows[0]['expired_present']==0
            if state.get('historical_gaps'):
                assert rows[0]['expired_rows']==90
        state.setdefault('imputation_reports',{})[stage]=rows[0]
        return rows[0]

    published_imputation=imputation_report(current,'bounded_calendar_v1','published')
    if state.get('revision_cases'):
        preserved=query(bq,state,f'''SELECT measurement,preserved_prior_value,latest_revision_missing,source_missing
          FROM `{PROJECT}.{n["studio"]}.ls_series_quality`
          WHERE date=DATE_SUB(DATE '{state["business_date"]}',INTERVAL 1 DAY) ORDER BY measurement''',name='missing_revision_report')
        assert len(preserved)==4 and sum(row['preserved_prior_value'] for row in preserved)==3
        assert all(row['latest_revision_missing'] and row['source_missing'] for row in preserved if row['preserved_prior_value'])
        state['preserved_missing_revision_report']=preserved
    preview = invoke(seed)
    assert not preview['applied'] and preview['target']['current_release']==current
    assert fingerprints(bq,state)==state['published_fingerprints']
    view = bq.get_table(f'{PROJECT}.{n["studio"]}.ls_freshness',timeout=30)
    assert 'FRED' not in view.view_query and 'stream_staging' not in view.view_query
    assert [f.name for f in view.schema]==['feed','last_ingestion_time','minutes_ago','hours_ago','source_name','source_url']
    freshness = query(bq,state,f'SELECT * FROM `{PROJECT}.{n["studio"]}.ls_freshness`',name='freshness_published')
    assert len(freshness)==3 and all(row['last_ingestion_time'] is not None for row in freshness)
    assert next(r for r in freshness if r['feed']=='Oil Price')['source_name']=='U.S. Energy Information Administration (EIA)'

    # Freeze a second valid candidate against the first accepted release.
    stale = hashlib.sha256((current+'/stale-base-proof').encode()).hexdigest()
    acquired = datetime.fromisoformat(state['fixture_acquired_at'])
    started = acquired-timedelta(seconds=1)
    sources = json.loads(json.dumps(state['source_receipts']))
    for feed,table in [('cocoa','cocoa_prices'),('oil','oil_prices'),('weather','precipitation_moisture')]:
        old = sources[feed]['acquisition_id']
        identity = hashlib.sha256((stale+'/'+feed).encode()).hexdigest()
        sources[feed].update(acquisition_id=identity,acquired_at=acquired.isoformat())
        measure = {'cocoa':'cocoa_price','oil':'oil_price','weather':'precipitation,soil_moisture'}[feed]
        sql = f'''INSERT `{PROJECT}.{n["staging"]}.{table}`
          SELECT date,{measure},@acquired,raw_payload,@acquired,CAST(@acquired AS STRING),
            TIMESTAMP_ADD(@acquired,INTERVAL 2 SECOND),CAST(NULL AS TIMESTAMP),'valid',
            delivery_id,pubsub_message_id,@identity,LOWER(TO_HEX(SHA256(CONCAT(observation_id,'stale-base-proof'))))
          FROM `{PROJECT}.{n["staging"]}.{table}` WHERE acquisition_id=@original AND delivery_id!='late-fixture' '''
        query(bq,state,sql,params=[bigquery.ScalarQueryParameter('acquired','TIMESTAMP',acquired),
              bigquery.ScalarQueryParameter('identity','STRING',identity),bigquery.ScalarQueryParameter('original','STRING',old)],name='stage_stale_'+feed)
        sources[feed]['observation_manifest']=source_manifest(bq,state,feed,table,identity)

    def stale_call(stage):
        params = [bigquery.ScalarQueryParameter('release','STRING',stale),
                  bigquery.ScalarQueryParameter('business','DATE',date.fromisoformat(state['business_date'])),
                  bigquery.ScalarQueryParameter('run','STRING','stale_fixture_'+state['token']),
                  bigquery.ScalarQueryParameter('started','TIMESTAMP',started),
                  bigquery.ScalarQueryParameter('sources','STRING',json.dumps(sources,sort_keys=True,separators=(',',':'))),
                  bigquery.ScalarQueryParameter('code','STRING',state['code_id']),
                  bigquery.ScalarQueryParameter('stage','STRING',stage)]
        return query(bq,state,f'CALL `{PROJECT}.{n["delivery"]}.run_daily_pipeline`(@release,@business,@run,@started,@sources,@code,@stage)',
                     params=params,name='stale_'+str(stage),timeout=240)
    try:
        stale_call('after_candidates')
    except BadRequest as error:
        if 'Injected failure after isolated candidates' not in str(error):
            raise
    else:
        raise AssertionError('Unpublished stale candidate unexpectedly succeeded')
    assert fingerprints(bq,state)==state['published_fingerprints']
    state['stale_candidate_release']=stale
    save(state)
    recovered = invoke(seed,current,state['token']+'-restore-seed',apply=True)
    assert recovered['applied'] and fingerprints(bq,state)==state['baseline']
    imputation_report(seed,'legacy_unbounded_or_unknown','rollback')
    repeated = invoke(seed,current,state['token']+'-restore-seed',apply=True)
    assert repeated['job_id']==recovered['job_id'] and fingerprints(bq,state)==state['baseline']
    try:
        stale_call(None)
    except BadRequest as error:
        if 'Another release was published' not in str(error):
            raise
    else:
        raise AssertionError('A frozen candidate overwrote a different active release')
    assert fingerprints(bq,state)==state['baseline']
    assert all(row['last_ingestion_time'] is None for row in query(bq,state,
        f'SELECT * FROM `{PROJECT}.{n["studio"]}.ls_freshness`',name='legacy_timestamps_not_invented'))
    try:
        invoke(current,current,state['token']+'-wrong-current',apply=True)
    except BadRequest as error:
        if 'Current release changed' not in str(error):
            raise
    else:
        raise AssertionError('Stale expected-current marker was accepted')
    assert fingerprints(bq,state)==state['baseline']

    # Alter one retained candidate, prove rejection, then restore its exact float.
    backup = query(bq,state,f'''SELECT forecast_date,forecast_cocoa_price FROM `{PROJECT}.{n["delivery"]}.forecast_40d`
      WHERE release_id=@release ORDER BY forecast_date LIMIT 1''',
      params=[bigquery.ScalarQueryParameter('release','STRING',current)],name='retained_artifact_backup')[0]
    state['retained_artifact_backup']=backup
    save(state)
    params=[bigquery.ScalarQueryParameter('release','STRING',current),
            bigquery.ScalarQueryParameter('date','DATE',backup['forecast_date'])]
    query(bq,state,f'UPDATE `{PROJECT}.{n["delivery"]}.forecast_40d` SET forecast_cocoa_price=forecast_cocoa_price+1 WHERE release_id=@release AND forecast_date=@date',
          params=params,name='corrupt_retained_candidate')
    try:
        invoke(current,seed,state['token']+'-corrupt-artifact',apply=True)
    except BadRequest as error:
        if 'Retained rollback artifacts are incomplete or altered' not in str(error):
            raise
    else:
        raise AssertionError('Altered retained artifacts were published')
    assert fingerprints(bq,state)==state['baseline']
    query(bq,state,f'UPDATE `{PROJECT}.{n["delivery"]}.forecast_40d` SET forecast_cocoa_price=@value WHERE release_id=@release AND forecast_date=@date',
          params=[*params,bigquery.ScalarQueryParameter('value','FLOAT64',backup['forecast_cocoa_price'])],name='restore_retained_candidate')
    final = invoke(current,seed,state['token']+'-restore-published',apply=True)
    assert final['applied'] and fingerprints(bq,state)==state['published_fingerprints']
    assert imputation_report(current,'bounded_calendar_v1','restored')==published_imputation
    state.update(phase='recovery_verified',rollback_to_baseline=recovered,restoration_to_published=final,
                 rollback_retry_is_noop=True,stale_expected_current_rejected=True,altered_retained_artifacts_rejected=True,
                 stale_frozen_candidate_rejected=True,accepted_release_freshness_verified=True)
    save(state)
    print(json.dumps({'phase':'recovery_verified','token':state['token'],'six_table_rollback':True,
                      'stale_candidate_blocked':True,'altered_recovery_blocked':True,'freshness_matches_active_release':True}),flush=True)


def main():
    global RECEIPT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=('prepare', 'exercise', 'smoke', 'rollback', 'cleanup'))
    parser.add_argument('--receipt', required=True, type=Path)
    parser.add_argument('--historical-gaps', action='store_true', help='Seed prolonged historical gaps during isolated preparation')
    parser.add_argument('--revision-cases', action='store_true', help='Seed equivalent baseline/staging repeats and missing revisions')
    args = parser.parse_args()
    if args.historical_gaps and args.phase!='prepare':
        parser.error('--historical-gaps is only valid for prepare')
    if args.revision_cases and args.phase!='prepare':
        parser.error('--revision-cases is only valid for prepare')
    RECEIPT = args.receipt
    {'prepare': lambda path: prepare(path,args.historical_gaps,args.revision_cases), 'exercise': exercise, 'smoke': lambda path: exercise(path, False),
     'rollback': rollback, 'cleanup': cleanup}[args.phase](args.receipt)


if __name__ == '__main__':
    main()
