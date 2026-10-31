"""Check BI source metadata and dry-run chart-shaped SQL; never execute data queries."""

import argparse
import json
from pathlib import Path
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[2]
CONTRACT = ROOT / 'bi/looker_studio_report.json'


def source_links(contract):
    """Create separate connector links; existing report aliases are not known."""
    result = {}
    for view in contract['sources']:
        result[view] = 'https://lookerstudio.google.com/reporting/create?' + urlencode({
            'c.mode': 'edit', 'r.reportName': 'Cocoa - ' + view,
            'ds.ds0.connector': 'bigQuery', 'ds.ds0.datasourceName': view,
            'ds.ds0.projectId': contract['project'], 'ds.ds0.billingProjectId': contract['project'],
            'ds.ds0.type': 'TABLE', 'ds.ds0.datasetId': contract['dataset'], 'ds.ds0.tableId': view})
    return result


def validate(client, contract):
    from google.cloud import bigquery

    sources = {}
    for view, required in contract['sources'].items():
        table = client.get_table(f"{contract['project']}.{contract['dataset']}.{view}", timeout=30)
        fields = {field.name: field.field_type for field in table.schema}
        missing = sorted(set(required) - fields.keys())
        if table.table_type != 'VIEW' or missing:
            raise ValueError(f'BI view contract mismatch for {view}: type={table.table_type}, missing={missing}')
        sources[view] = {key: fields[key] for key in required}
    probes = {}
    for name, query in contract['probes'].items():
        job = client.query(query, location=contract['location'], job_config=bigquery.QueryJobConfig(
            dry_run=True, use_query_cache=False,
            labels={'app': 'cocoa-prices', 'component': 'bi-preflight'}))
        if not job.dry_run:
            raise RuntimeError('Expected a dry run')
        probes[name] = {'estimated_bytes_processed': job.total_bytes_processed,
                        'dry_run': True}
    return {'sources': sources, 'probes': probes, 'report_rendered': False,
            'actual_report_query_cost_measured': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path, help='New ignored/private JSON receipt')
    parser.add_argument('--links-only', action='store_true', help='Create connector links without credentials or API calls')
    args = parser.parse_args()
    if args.output.exists():
        parser.error('Output must not exist')
    contract = json.loads(CONTRACT.read_text(encoding='utf-8'))
    result = {'report_id': contract['report_id'], 'contract_version': contract['contract_version'],
              'connector_links': source_links(contract)}
    if not args.links_only:
        import google.auth
        from google.cloud import bigquery

        credentials, _ = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform'], quota_project_id=contract['project'])
        result.update(validate(bigquery.Client(project=contract['project'], credentials=credentials), contract))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as handle:
        json.dump(result, handle, indent=2)
    print(json.dumps({'output': str(args.output), 'metadata_and_dry_runs': not args.links_only,
                      'rendered_report_verified': False}))


if __name__ == '__main__':
    main()
