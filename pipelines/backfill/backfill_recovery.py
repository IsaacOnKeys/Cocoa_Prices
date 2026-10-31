"""Retain rejected historical inputs in the existing STRING Errors column."""

import hashlib
import json
from pathlib import Path
from datetime import datetime, timezone

SOURCES = {
    'cocoa': ('gs://raw-historic-data/snapshots/2025-10-02/icco/Daily_Prices_Home.csv', ('Date', 'Euro_Price'), 'icco-csv-v1'),
    'oil': ('gs://raw-historic-data/snapshots/2026-10-27/eia/brent_oil_eia.json', ('date', 'brent_price_eu'), 'eia-published-json-v1'),
    'weather': ('gs://raw-historic-data/snapshots/2025-10-02/nasa_power/POWER_Point_Daily.csv', ('date', 'precipitation', 'soil_moisture'), 'nasa-csv-v1'),
}


def configure_worker_package(options, script_file):
    """Stage the shared module when Beam launches remote Dataflow workers."""
    from apache_beam.options.pipeline_options import SetupOptions, StandardOptions

    if options.view_as(StandardOptions).runner == 'DataflowRunner':
        setup = options.view_as(SetupOptions)
        if not setup.setup_file:
            setup.setup_file = str(Path(script_file).with_name('setup.py').resolve())


def invalid_record(record, feed, request=None):
    uri, fields, version = SOURCES[feed]
    # Raw text is itself a STRING: even malformed JSON/NaN remains replayable.
    original = record.get('_raw_line') or json.dumps(record, default=str)
    evidence = {'version': 1, 'source_uri': uri, 'source_schema_version': version,
                'source_acquired_at': None, 'rejected_at': datetime.now(timezone.utc).isoformat(),
                'error_category': 'historical_validation', 'errors': record.get('Errors', record.get('_parse_error', 'invalid')),
                'raw_payload': original, 'input_sha256': hashlib.sha256(original.encode()).hexdigest()}
    if request is not None:
        evidence.update(source_uri=request.input_uri, source_generation=str(request.generation),
                        source_sha256=request.sha256, source_acquired_at=request.acquired_at,
                        backfill_run=request.dataset, transform_sha256=getattr(request, 'transform_hashes', None))
    return {**{key: None if record.get(key) is None else str(record[key]) for key in fields},
            'Errors': json.dumps(evidence, sort_keys=True, separators=(',', ':'), allow_nan=False)}


def resolve_duplicates(records, feed):
    """Collapse identical facts; no order can make a conflicting fact authoritative."""
    fields = SOURCES[feed][1]
    ordered = sorted((dict(row) for row in records),
                     key=lambda row: json.dumps(row, sort_keys=True, allow_nan=False))
    facts = {json.dumps({key: row[key] for key in fields}, sort_keys=True, allow_nan=False)
             for row in ordered}
    if not ordered:
        return [], []
    conflict = len(facts) != 1
    rejected = ordered if conflict else ordered[1:]
    reason = 'Conflicting source revisions for date' if conflict else 'Duplicate identical source fact'
    return ([] if conflict else [ordered[0]], [{**row, 'Errors': reason} for row in rejected])
