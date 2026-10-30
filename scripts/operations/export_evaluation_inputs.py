"""Export one accepted raw observation snapshot for local evaluation; never train in GCP."""

import argparse
import csv
import hashlib
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--release', required=True)
    parser.add_argument('--business-date', required=True, type=date.fromisoformat)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if not re.fullmatch(r'[a-f0-9]{64}', args.release):
        parser.error('Expected the exact accepted release SHA256')
    metadata_path = args.output.with_suffix('.metadata.json')
    if args.output.exists() or metadata_path.exists():
        parser.error('Refusing to overwrite an input snapshot or metadata')
    from google.cloud import bigquery

    client = bigquery.Client(project='cocoa-prices-430315', location='europe-west3')
    parameters = [bigquery.ScalarQueryParameter('release', 'STRING', args.release),
                  bigquery.ScalarQueryParameter('business', 'DATE', args.business_date)]
    config = bigquery.QueryJobConfig(maximum_bytes_billed=52428800, query_parameters=parameters)
    registry_job = client.query('''SELECT release_id,business_date,input_cutoff,published_at,code_id,model_id
      FROM `cocoa-prices-430315.analytics_delivery.release_runs`
      WHERE release_id=@release AND business_date=@business AND status='published' ''',
      job_config=config, retry=None, job_retry=None, timeout=20)
    records = [dict(row) for row in registry_job.result(timeout=30, job_retry=None)]
    if len(records) != 1:
        raise ValueError('Expected one accepted published release')
    sql = '''WITH cocoa AS (
      SELECT Date AS date,Euro_Price AS cocoa_price FROM `cocoa-prices-430315.analytics_delivery.cocoa`
      WHERE release_id=@release AND business_date=@business
    ), oil AS (
      SELECT date,brent_price_eu AS oil_price FROM `cocoa-prices-430315.analytics_delivery.brent_prices`
      WHERE release_id=@release AND business_date=@business
    ), weather AS (
      SELECT date,precipitation,soil_moisture FROM `cocoa-prices-430315.analytics_delivery.precipitation`
      WHERE release_id=@release AND business_date=@business
    ), dates AS (
      SELECT date FROM cocoa UNION DISTINCT SELECT date FROM oil UNION DISTINCT SELECT date FROM weather
    )
    SELECT d.date,c.cocoa_price,o.oil_price,w.precipitation,w.soil_moisture
    FROM dates d LEFT JOIN cocoa c USING(date) LEFT JOIN oil o USING(date) LEFT JOIN weather w USING(date)
    ORDER BY date'''
    job = client.query(sql, job_config=config, retry=None, job_retry=None, timeout=20)
    rows = list(job.result(timeout=40, job_retry=None))
    with args.output.open('x', encoding='utf-8', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(['date', 'cocoa_price', 'oil_price', 'precipitation', 'soil_moisture'])
        writer.writerows([row['date'].isoformat(), *[row[key] for key in
                         ('cocoa_price', 'oil_price', 'precipitation', 'soil_moisture')]] for row in rows)
    metadata = {**records[0], 'exported_at': datetime.now(timezone.utc).isoformat(), 'rows': len(rows),
                'input_sha256': hashlib.sha256(args.output.read_bytes()).hexdigest(),
                'registry_job': registry_job.job_id, 'export_job': job.job_id,
                'bytes_billed': (registry_job.total_bytes_billed or 0)+(job.total_bytes_billed or 0),
                'historical_vintages_verified': False}
    metadata_path.write_text(json.dumps(metadata, indent=2, default=str), encoding='utf-8')
    print(json.dumps({'rows': len(rows), 'bytes_billed': metadata['bytes_billed'], 'output': str(args.output)}))


if __name__ == '__main__':
    main()
