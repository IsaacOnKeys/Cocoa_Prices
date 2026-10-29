"""Preview a retained accepted release; restore only with an explicit current marker and incident key."""

import argparse
import hashlib
import json
import re


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('target', help='Exact retained release SHA256')
    parser.add_argument('--project', default='cocoa-prices-430315')
    parser.add_argument('--delivery', default='analytics_delivery')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--expected-current')
    parser.add_argument('--incident-key')
    parser.add_argument('--reason')
    args = parser.parse_args()
    if (not re.fullmatch(r'[a-f0-9]{64}', args.target)
            or not re.fullmatch(r'[a-z][a-z0-9-]{4,29}', args.project)
            or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,1023}', args.delivery)
            or (args.apply and (not re.fullmatch(r'[a-f0-9]{64}', args.expected_current or '')
                               or not args.incident_key or not 1 <= len(args.incident_key) <= 1024
                               or not args.reason or not 1 <= len(args.reason) <= 1024))
            or (not args.apply and (args.expected_current or args.incident_key or args.reason))):
        parser.error('Pin the target; --apply also requires expected current SHA, bounded incident key and reason')
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    bq = bigquery.Client(project=args.project, location='europe-west3')
    namespace = args.project+'.'+args.delivery
    config = bigquery.QueryJobConfig(maximum_bytes_billed=1073741824,
        query_parameters=[bigquery.ScalarQueryParameter('target','STRING',args.target)])
    sql = f'''SELECT r.release_id,r.business_date,r.status,r.model_id,r.code_id,r.quality_results,
      (SELECT release_id FROM `{namespace}.active_release` WHERE singleton=1) AS current_release
      FROM `{namespace}.release_runs` r WHERE r.release_id=@target'''
    rows = [dict(row) for row in bq.query(sql, job_config=config).result(timeout=30)]
    if len(rows) != 1 or rows[0]['status'] not in {'published', 'legacy_verified'}:
        raise ValueError('Target is not a single retained accepted release')
    result = {'target': rows[0], 'applied': False}
    if args.apply:
        event = hashlib.sha256(args.incident_key.encode()).hexdigest()
        params = [bigquery.ScalarQueryParameter('target','STRING',args.target),
                  bigquery.ScalarQueryParameter('current','STRING',args.expected_current),
                  bigquery.ScalarQueryParameter('event','STRING',event),
                  bigquery.ScalarQueryParameter('reason','STRING',args.reason)]
        sql = f'CALL `{namespace}.rollback_release`(@target,@current,@event,@reason)'
        config = bigquery.QueryJobConfig(query_parameters=params, maximum_bytes_billed=1073741824)
        job_id = 'cocoa_rollback_'+event
        try:
            job = bq.get_job(job_id, location='europe-west3', retry=None, timeout=30)
        except NotFound:
            job = bq.query(sql, job_config=config, job_id=job_id, retry=None, job_retry=None, timeout=30)
        actual = job.to_api_repr()['configuration']['query']
        expected = config.to_api_repr()['query']
        if actual['query'] != sql or actual.get('queryParameters') != expected['queryParameters']:
            raise ValueError('Incident key already identifies a different rollback request')
        result.update(applied=True, event_id=event, job_id=job_id,
                      active_release=[dict(row) for row in job.result(timeout=180, job_retry=None)])
    print(json.dumps(result, default=str))


if __name__ == '__main__':
    main()
