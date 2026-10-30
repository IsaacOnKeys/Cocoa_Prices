"""Manually train, review/select or restore a model. Default operation is preview-only."""

import argparse
import hashlib
import json
import math
import re
from datetime import date, timedelta
from pathlib import Path

PROJECT = 'cocoa-prices-430315'
DELIVERY = PROJECT+'.analytics_delivery'
MODELS = PROJECT+'.models'
MINIMUM_TRAINING = 365
MINIMUM_HOLDOUT = 20
MAXIMUM_DEGRADATION = 1.05
FEATURES = {'y','y_lag1','y_lag7','y_lag28','oil_ma7','precip_ma7','soil_ma7','dow','mon'}


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def scope(source, business, cutoff, key):
    if (not re.fullmatch(r'[a-f0-9]{64}',source) or not 1 <= len(key) <= 128
            or cutoff+timedelta(days=40) >= business):
        raise ValueError('Pin the source and a cutoff leaving a mature holdout interval')
    identity = digest([source,business.isoformat(),cutoff.isoformat(),key])
    return {'source_release_id':source,'business_date':business.isoformat(),'training_cutoff':cutoff.isoformat(),
            'candidate_key':key,'model_id':MODELS+'.cocoa_candidate_'+identity,
            'training_job_id':'cocoa_candidate_train_'+identity}


def train_sql(model):
    if not re.fullmatch(re.escape(MODELS)+r'\.cocoa_candidate_[a-f0-9]{64}',model):
        raise ValueError('Expected an immutable candidate model ID')
    return f'''CREATE MODEL `{model}`
      OPTIONS(model_type='linear_reg',input_label_cols=['target_40'],l2_reg=1.0,data_split_method='NO_SPLIT')
      AS SELECT * EXCEPT(release_id,business_date,date) FROM `{DELIVERY}.features`
      WHERE release_id=@source AND business_date=@business AND target_40 IS NOT NULL
        AND DATE_ADD(date,INTERVAL 41 DAY)<=@cutoff'''


def evaluation_sql(model):
    train_sql(model)  # Same strict identifier validation.
    return f'''WITH predictions AS (
      SELECT * FROM ML.PREDICT(MODEL `{model}`,(
        SELECT * EXCEPT(release_id,business_date,target_40) FROM `{DELIVERY}.features`
        WHERE release_id=@source AND business_date=@business AND date>@cutoff
          AND DATE_ADD(date,INTERVAL 40 DAY)<=@business AND EXTRACT(DAYOFWEEK FROM date) BETWEEN 2 AND 6
      ))
    ), paired AS (
      SELECT p.predicted_target_40 AS prediction,c.Euro_Price AS actual,p.y AS persistence,s.cocoa_price AS seasonal
      FROM predictions p JOIN `{DELIVERY}.cocoa` c
        ON c.release_id=@source AND c.business_date=@business AND c.Date=DATE_ADD(p.date,INTERVAL 40 DAY)
      JOIN `{DELIVERY}.joined_daily_cocoa_oil_weather` s
        ON s.release_id=@source AND s.business_date=@business AND s.date=DATE_SUB(p.date,INTERVAL 2 DAY)
      WHERE p.y IS NOT NULL AND s.cocoa_price IS NOT NULL AND c.Euro_Price IS NOT NULL
    )
    SELECT COUNT(*) AS holdout_rows,
      COUNTIF(prediction IS NULL OR IS_NAN(prediction) OR IS_INF(prediction) OR prediction<=0) AS invalid_predictions,
      AVG(IF(prediction>0 AND NOT IS_NAN(prediction) AND NOT IS_INF(prediction),ABS(prediction-actual),NULL)) AS model_mae,
      AVG(ABS(persistence-actual)) AS persistence_mae,AVG(ABS(seasonal-actual)) AS seasonal_mae
    FROM paired'''


def review_gate(training_rows, metrics):
    if training_rows < MINIMUM_TRAINING or metrics['holdout_rows'] < MINIMUM_HOLDOUT:
        raise ValueError('Insufficient training or matched held-out samples')
    if metrics['invalid_predictions']:
        raise ValueError('Candidate produced invalid held-out predictions')
    values = [metrics[key] for key in ('model_mae','persistence_mae','seasonal_mae')]
    if any(value is None or not math.isfinite(value) or value < 0 for value in values):
        raise ValueError('Invalid candidate evaluation metrics')
    if values[0] > MAXIMUM_DEGRADATION*min(values[1:]):
        raise ValueError('Candidate MAE is more than 5% worse than the better naive baseline')


def verify_model(model, expected_etag=None):
    if (model.model_type != 'LINEAR_REGRESSION' or {field.name for field in model.feature_columns} != FEATURES
            or [field.name for field in model.label_columns] != ['predicted_target_40']):
        raise ValueError('Model type, feature schema or label differs from the reviewed contract')
    if expected_etag is not None and model.etag != expected_etag:
        raise ValueError('Candidate model changed after its evaluation receipt')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command',required=True)
    train = sub.add_parser('train')
    train.add_argument('--source-release',required=True)
    train.add_argument('--business-date',required=True,type=date.fromisoformat)
    train.add_argument('--cutoff',required=True,type=date.fromisoformat)
    train.add_argument('--key',required=True)
    train.add_argument('--output',required=True,type=Path)
    promote = sub.add_parser('promote')
    promote.add_argument('receipt',type=Path)
    promote.add_argument('--acknowledge-revised-history',action='store_true')
    restore = sub.add_parser('restore')
    restore.add_argument('review',help='Exact prior approved review SHA256')
    for command in (promote,restore):
        command.add_argument('--expected-review',required=True)
        command.add_argument('--reviewer',required=True)
        command.add_argument('--reason',required=True)
        command.add_argument('--key',required=True)
    for command in (train,promote,restore):
        command.add_argument('--execute',action='store_true')
    args = parser.parse_args()
    if args.command == 'train':
        request = scope(args.source_release,args.business_date,args.cutoff,args.key)
        if args.output.exists():
            parser.error('Refusing to overwrite a candidate receipt')
    else:
        if (not re.fullmatch(r'[a-f0-9]{64}',args.expected_review) or not 1 <= len(args.reviewer) <= 128
                or not 1 <= len(args.reason) <= 1024 or not 1 <= len(args.key) <= 128):
            parser.error('Pin the expected review and supply bounded reviewer, reason and decision key')
        request = json.loads(args.receipt.read_text()) if args.command=='promote' else {'restore_review':args.review}
        if args.command=='restore' and not re.fullmatch(r'[a-f0-9]{64}',args.review):
            parser.error('Expected prior approved review SHA256')
        if args.command=='promote' and not args.acknowledge_revised_history:
            parser.error('Review the historical-vintage limitation and acknowledge it explicitly')
    if not args.execute:
        print(json.dumps({'operation':args.command,'executed':False,'request':request,
                          'policy':'manual training; separate reviewed promotion; no production model changes in preview'}))
        return
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT,location='europe-west3')

    def query(sql, parameters=(), job_id=None, limit=134217728):
        config = bigquery.QueryJobConfig.from_api_repr({'jobTimeoutMs':'600000',
            'labels':{'app':'cocoa-prices','component':'manual-model-review'},
            'query':{'maximumBytesBilled':str(limit),'useLegacySql':False,
                     'queryParameters':[bigquery.ScalarQueryParameter(*value).to_api_repr() for value in parameters]}})
        try:
            job = client.get_job(job_id,location='europe-west3',retry=None,timeout=20) if job_id else None
        except NotFound:
            job = None
        if job is None:
            job = client.query(sql,job_config=config,job_id=job_id,retry=None,job_retry=None,timeout=20)
        actual = job.to_api_repr()['configuration']['query']
        if actual['query'] != sql or actual.get('queryParameters',[]) != config.to_api_repr()['query'].get('queryParameters',[]):
            raise ValueError('Job identity belongs to a different request')
        return job,list(job.result(timeout=600,job_retry=None))

    def evaluate(card):
        validated = scope(card['source_release_id'],date.fromisoformat(card['business_date']),
                          date.fromisoformat(card['training_cutoff']),card['candidate_key'])
        if any(card[key] != validated[key] for key in validated):
            raise ValueError('Candidate receipt identity is inconsistent')
        params = [('source','STRING',card['source_release_id']),('business','DATE',card['business_date']),
                  ('cutoff','DATE',card['training_cutoff'])]
        _, rows = query(f'''SELECT COUNT(*) AS n FROM `{DELIVERY}.features` f
          JOIN `{DELIVERY}.release_runs` r USING(release_id,business_date)
          WHERE f.release_id=@source AND f.business_date=@business AND r.status='published'
            AND target_40 IS NOT NULL AND DATE_ADD(date,INTERVAL 41 DAY)<=@cutoff''',params)
        count = rows[0]['n']
        _,rows = query(f'''SELECT COUNT(*) AS n FROM `{DELIVERY}.features` f
          JOIN `{DELIVERY}.cocoa` c ON c.release_id=f.release_id AND c.business_date=f.business_date
            AND c.Date=DATE_ADD(f.date,INTERVAL 40 DAY)
          JOIN `{DELIVERY}.joined_daily_cocoa_oil_weather` s ON s.release_id=f.release_id
            AND s.business_date=f.business_date AND s.date=DATE_SUB(f.date,INTERVAL 2 DAY)
          WHERE f.release_id=@source AND f.business_date=@business AND f.date>@cutoff
            AND DATE_ADD(f.date,INTERVAL 40 DAY)<=@business AND EXTRACT(DAYOFWEEK FROM f.date) BETWEEN 2 AND 6
            AND f.y IS NOT NULL AND s.cocoa_price IS NOT NULL AND c.Euro_Price IS NOT NULL''',params)
        if rows[0]['n'] < MINIMUM_HOLDOUT:
            raise ValueError('Insufficient matched holdout origins; no training is started')
        return params,count

    if args.command in {'train','promote'}:
        params,count = evaluate(request)
        if count < MINIMUM_TRAINING:
            raise ValueError('Insufficient eligible training rows')
        if args.command=='train':
            training, _ = query(train_sql(request['model_id']),params,request['training_job_id'])
        else:
            training = client.get_job(request['training_job_id'],location='europe-west3',retry=None,timeout=20)
            actual = training.to_api_repr()['configuration']['query']
            expected = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter(*value) for value in params]).to_api_repr()['query']
            if (training.state!='DONE' or training.error_result or actual['query']!=train_sql(request['model_id'])
                    or actual.get('queryParameters')!=expected.get('queryParameters')):
                raise ValueError('Training job does not prove the declared source/cutoff')
        model = client.get_model(request['model_id'],retry=None,timeout=20)
        verify_model(model,request.get('model_etag'))
        evaluation, rows = query(evaluation_sql(request['model_id']),params,
                                 'cocoa_candidate_eval_'+request['model_id'].rsplit('_',1)[-1])
        metrics = dict(rows[0])
        card = {**request,'model_etag':model.etag,'training_rows':count,'evaluation':metrics,
                'evaluation_job_id':evaluation.job_id,'model_type':model.model_type,
                'feature_contract':'nine_predictor_40_day_v1','retraining_policy':'manual_only',
                'performance_status':'provisional_revised_history_holdout',
                'historical_vintages_verified':False,'training_config':{'l2_reg':1.0,'data_split_method':'NO_SPLIT'},
                'thresholds':{'minimum_training':MINIMUM_TRAINING,'minimum_holdout':MINIMUM_HOLDOUT,
                              'maximum_baseline_mae_ratio':MAXIMUM_DEGRADATION}}
        card.pop('gate_failure',None)
        try:
            review_gate(count,metrics)
            card['gate_passed'] = True
        except ValueError as error:
            card.update(gate_passed=False,gate_failure=str(error))
        if args.command=='train':
            args.output.write_text(json.dumps(card,indent=2),encoding='utf-8')
            print(json.dumps({'trained':True,'promoted':False,'gate_passed':card['gate_passed'],'receipt':str(args.output)}))
            return
        review_gate(count,metrics)
    else:
        _,rows = query(f'SELECT model_card FROM `{DELIVERY}.model_reviews` WHERE review_id=@review',
                     [('review','STRING',args.review)])
        if len(rows)!=1:
            raise ValueError('Expected one retained reviewed model')
        value = rows[0]['model_card']
        card = json.loads(value) if isinstance(value,str) else value
        verify_model(client.get_model(card['model_id'],retry=None,timeout=20),card['model_etag'])
    review = digest([args.expected_review,card['model_id'],args.key])
    card = {**card,'review_id':review,'reviewer':args.reviewer,'review_reason':args.reason}
    params = [('expected','STRING',args.expected_review),('review','STRING',review),('model','STRING',card['model_id']),
              ('reviewer','STRING',args.reviewer),('reason','STRING',args.reason),
              ('card','STRING',json.dumps(card,sort_keys=True,separators=(',',':')))]
    job,_ = query(f'CALL `{DELIVERY}.select_reviewed_model`(@expected,@review,@model,@reviewer,@reason,@card)',
                  params,'cocoa_model_review_'+review)
    print(json.dumps({'selected_for_new_releases':card['model_id'],'review_id':review,'job_id':job.job_id}))


if __name__ == '__main__':
    main()
