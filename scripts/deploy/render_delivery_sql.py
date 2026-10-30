"""Render the fixed analytics-delivery SQL identifiers without accepting SQL fragments."""

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULTS = {'project': 'cocoa-prices-430315', 'delivery': 'analytics_delivery',
            'curated': 'cocoa_related', 'staging': 'stream_staging', 'models': 'models', 'studio': 'cocoa_related_studio'}
FILES = {'migration': ROOT / 'sql/migrations/020_atomic_delivery.sql',
         'run_manifest_migration': ROOT / 'sql/migrations/019_run_manifests.sql',
         'procedure': ROOT / 'sql/procedures/run_analytics_release.sql',
         'rollback': ROOT / 'sql/procedures/rollback_analytics_release.sql',
         'serving_views': ROOT / 'sql/serving/analytics_delivery.sql',
         'product_status_view': ROOT / 'sql/serving/product_delivery_status.sql',
         'forecast_history_views': ROOT / 'sql/serving/forecast_history.sql',
         'forecast_product_view': ROOT / 'sql/serving/forecast_product.sql',
         'issued_evaluation_views': ROOT / 'sql/serving/issued_forecast_evaluation.sql',
         'imputation_view': ROOT / 'sql/serving/imputation.sql',
         'retire_legacy': ROOT / 'sql/procedures/run_daily_pipeline.sql',
         'bootstrap': ROOT / 'sql/migrations/020_atomic_delivery_bootstrap.sql'}
FRAGMENTS = {'input_quality_checks': ROOT / 'sql/quality/input_checks.sql',
             'output_quality_checks': ROOT / 'sql/quality/output_checks.sql',
             'imputation_join': ROOT / 'sql/01_curated_data/bounded_join.sql',
             'revision_selection': ROOT / 'sql/01_curated_data/select_revisions.sql'}
FILES.update(FRAGMENTS)


def render(kind, **identifiers):
    context = {**DEFAULTS, **identifiers}
    if set(context) != set(DEFAULTS) or not re.fullmatch(r'[a-z][a-z0-9-]{4,29}', context['project']):
        raise ValueError('Invalid delivery SQL project/identifier keys')
    if any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,1023}', context[k]) for k in context if k != 'project'):
        raise ValueError('Invalid delivery SQL dataset identifier')
    if len({context['delivery'], context['curated'], context['staging']}) != 3:
        raise ValueError('Delivery, curated and staging datasets must be distinct')
    text = FILES[kind].read_text(encoding='utf-8')
    for key, path in FRAGMENTS.items():
        if '${'+key+'}' in text:
            text = text.replace('${'+key+'}', path.read_text(encoding='utf-8'))
    if kind not in FRAGMENTS and '${project}.${delivery}.' not in text:
        raise ValueError('Delivery SQL must explicitly declare its target namespace')
    for key, value in context.items():
        text = text.replace('${'+key+'}', value)
    if re.search(r'\$\{[^}]+\}', text):
        raise ValueError('Unresolved delivery SQL identifier')
    return text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('kind', choices=FILES)
    parser.add_argument('--output', required=True, type=Path, help='Create a new local SQL file; never deploy automatically')
    for name, default in DEFAULTS.items():
        parser.add_argument('--'+name, default=default)
    args = parser.parse_args()
    with args.output.open('x', encoding='utf-8', newline='\n') as handle:
        handle.write(render(args.kind, **{key: getattr(args, key) for key in DEFAULTS}))


if __name__ == '__main__':
    main()
