"""Bounded, read-only workload/capacity snapshot; billing totals require a billing export/report."""

import argparse
import json
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote

PROJECT = 'cocoa-prices-430315'
NUMBER = '494039866722'
REGION = 'europe-west3'
QUERY = """
WITH jobs AS (
  SELECT DATE(creation_time,'Europe/Berlin') day,
    COALESCE((SELECT value FROM UNNEST(labels) WHERE key='component'),'unlabelled') component,
    IF(user_email='airflow-orchestrator@cocoa-prices-430315.iam.gserviceaccount.com','airflow','other') actor,
    COUNT(*) jobs,COUNTIF(error_result IS NOT NULL) failed_jobs,
    SUM(IFNULL(total_bytes_billed,0)) billed_bytes,
    SUM(IFNULL(total_bytes_processed,0)) processed_bytes,
    SUM(IFNULL(total_slot_ms,0)) slot_ms,
    MAX(TIMESTAMP_DIFF(end_time,start_time,MILLISECOND)) max_elapsed_ms,
    COUNTIF(reservation_id IS NOT NULL) capacity_priced_jobs
  FROM `region-europe-west3`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE creation_time>=TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL @days DAY)
    AND job_type='QUERY' AND state='DONE' AND statement_type IS DISTINCT FROM 'SCRIPT'
  GROUP BY 1,2,3
), sources AS (
  SELECT 'cocoa' feed,producer_acquired_at,acquisition_id,observation_id FROM `cocoa-prices-430315.stream_staging.cocoa_prices`
  UNION ALL SELECT 'oil',producer_acquired_at,acquisition_id,observation_id FROM `cocoa-prices-430315.stream_staging.oil_prices`
  UNION ALL SELECT 'weather',producer_acquired_at,acquisition_id,observation_id FROM `cocoa-prices-430315.stream_staging.precipitation_moisture`
), volume AS (
  SELECT DATE(producer_acquired_at,'Europe/Berlin') day,feed,COUNT(*) physical_rows,
    COUNT(DISTINCT acquisition_id) acquisitions,
    COUNT(DISTINCT CONCAT(acquisition_id,'/',observation_id)) acquisition_revisions,
    COUNTIF(acquisition_id IS NULL OR observation_id IS NULL) untraceable_rows
  FROM sources WHERE producer_acquired_at>=TIMESTAMP_SUB(CURRENT_TIMESTAMP(),INTERVAL @days DAY)
  GROUP BY 1,2
)
SELECT 'query_usage' kind,TO_JSON_STRING(t) payload FROM jobs t
UNION ALL SELECT 'source_volume',TO_JSON_STRING(t) FROM volume t
"""


def read(session, url, *, params=None, optional=False):
    response = session.get(url, params=params, timeout=30)
    if optional and response.status_code != 200:
        return {'unavailable_http_status': response.status_code}
    if response.status_code != 200:
        raise RuntimeError('Cost metadata request failed: HTTP ' + str(response.status_code))
    return response.json()


def metric(session, name, group, start, end, aligner):
    params = {'filter': 'metric.type="' + name + '"', 'interval.startTime': start,
              'interval.endTime': end, 'aggregation.alignmentPeriod': '86400s',
              'aggregation.perSeriesAligner': aligner, 'aggregation.crossSeriesReducer': 'REDUCE_SUM',
              'aggregation.groupByFields': group, 'pageSize': 1000}
    result = read(session, f'https://monitoring.googleapis.com/v3/projects/{PROJECT}/timeSeries',
                  params=params, optional=True)
    if result.get('nextPageToken'):
        raise RuntimeError('Metric snapshot exceeds bounded page size')
    return result


def table_sizes(client):
    """Portable API metadata fallback; this is not storage invoicing or a price scan."""
    tables = []
    for dataset in client.list_datasets(timeout=30, retry=None):
        tables.extend(client.list_tables(dataset.reference, max_results=201, timeout=30, retry=None))
        if len(tables) > 200:
            raise RuntimeError('Table metadata snapshot exceeds 200 tables')

    def size(item):
        table = client.get_table(item.reference, timeout=30, retry=None)
        return {'dataset': table.dataset_id, 'table': table.table_id, 'type': table.table_type,
                'logical_bytes': table.num_bytes, 'rows': table.num_rows}

    with ThreadPoolExecutor(max_workers=4) as pool:
        return list(pool.map(size, tables))


def warehouse_usage(client, days):
    from google.cloud import bigquery

    config = bigquery.QueryJobConfig(use_legacy_sql=False, maximum_bytes_billed=128 * 1024**2,
                query_parameters=[bigquery.ScalarQueryParameter('days', 'INT64', days)],
                labels={'app': 'cocoa-prices', 'component': 'finops-snapshot'})
    job = client.query(QUERY, job_config=config)
    rows = [{'kind': row['kind'], 'data': json.loads(row['payload'])} for row in job.result(timeout=60)]
    return {'warehouse_usage': rows, 'snapshot_query': {'job_id': job.job_id,
            'bytes_processed': job.total_bytes_processed, 'bytes_billed': job.total_bytes_billed,
            'cap_bytes': config.maximum_bytes_billed}, 'visible_table_storage': table_sizes(client),
            'storage_excludes_models_deleted_tables_and_time_travel': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='New private JSON receipt; never overwrite')
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--query-usage', action='store_true', help='Also query bounded job/storage metadata and staging identity counts; no prices')
    args = parser.parse_args()
    if not 1 <= args.days <= 31 or args.output.exists():
        parser.error('Use 1..31 days and a new output file')
    import google.auth
    from google.auth.transport.requests import AuthorizedSession
    from google.cloud import bigquery

    credentials, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'], quota_project_id=PROJECT)
    session = AuthorizedSession(credentials)
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=args.days)).isoformat()
    end = now.isoformat()
    root = f'https://compute.googleapis.com/compute/v1/projects/{PROJECT}'
    vm = read(session, root + f'/zones/{REGION}-c/instances/airflow-vm', params={
        'fields': 'name,status,machineType,disks(source,boot),labels,lastStartTimestamp,lastStopTimestamp'})
    disks = []
    for disk in vm['disks']:
        source = disk['source'].replace('https://www.googleapis.com/compute/v1/', 'https://compute.googleapis.com/compute/v1/')
        if not source.startswith(root + '/'):
            raise ValueError('Unexpected disk metadata target')
        disks.append(read(session, source, params={'fields': 'name,sizeGb,type,labels'}))
    services = read(session, f'https://run.googleapis.com/v2/projects/{PROJECT}/locations/{REGION}/services', params={
        'pageSize': 100, 'fields': 'services(name,labels,template(scaling,containers(resources))),nextPageToken'})
    if services.get('nextPageToken'):
        raise RuntimeError('Service inventory exceeds bounded page size')
    repositories = read(session, f'https://artifactregistry.googleapis.com/v1/projects/{PROJECT}/locations/{REGION}/repositories',
                        params={'pageSize': 100, 'fields': 'repositories(name,format,sizeBytes,cleanupPolicies,cleanupPolicyDryRun,labels,vulnerabilityScanningConfig),nextPageToken'}, optional=True)
    quota = read(session, f'https://serviceusage.googleapis.com/v1beta1/projects/{NUMBER}/services/bigquery.googleapis.com/consumerQuotaMetrics/'
                 + quote('bigquery.googleapis.com/quota/query/usage', safe=''), params={'view': 'FULL'}, optional=True)
    metrics = {}
    for name, group, aligner in (
        ('compute.googleapis.com/instance/uptime', 'resource.labels.instance_id', 'ALIGN_SUM'),
        ('run.googleapis.com/container/billable_instance_time', 'resource.labels.service_name', 'ALIGN_SUM'),
        ('run.googleapis.com/request_count', 'resource.labels.service_name', 'ALIGN_SUM'),
        ('storage.googleapis.com/storage/total_bytes', 'resource.labels.bucket_name', 'ALIGN_MAX'),
        ('logging.googleapis.com/billing/bytes_ingested', 'resource.labels.project_id', 'ALIGN_SUM'),
    ):
        metrics[name] = metric(session, name, group, start, end, aligner)
    result = {'observed_at': end, 'project': PROJECT, 'days': args.days, 'region': REGION,
              'vm': vm, 'disks': disks, 'regional_services': services, 'repositories': repositories,
              'query_quota': quota, 'metrics': metrics, 'billing_costs_verified': False}
    query_failed = False
    if args.query_usage:
        client = bigquery.Client(project=PROJECT, location=REGION, credentials=credentials)
        try:
            result.update(warehouse_usage(client, args.days))
        except Exception as error:
            query_failed = True
            result['warehouse_error'] = {'type': type(error).__name__}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as handle:
        json.dump(result, handle, indent=2, default=str)
    print(json.dumps({'output': str(args.output), 'vm': vm['status'], 'warehouse_usage_included': args.query_usage,
                      'billing_costs_verified': False, 'snapshot_query': result.get('snapshot_query'), 'partial': query_failed}))
    if query_failed:
        raise SystemExit('Warehouse usage incomplete; available metadata saved in the output receipt')


if __name__ == '__main__':
    main()
