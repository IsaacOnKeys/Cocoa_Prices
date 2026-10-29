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


def invalid_record(record, feed):
    uri, fields, version = SOURCES[feed]
    # Raw text is itself a STRING: even malformed JSON/NaN remains replayable.
    original = record.get('_raw_line') or json.dumps(record, default=str)
    evidence = {'version': 1, 'source_uri': uri, 'source_schema_version': version,
                'source_acquired_at': None, 'rejected_at': datetime.now(timezone.utc).isoformat(),
                'error_category': 'historical_validation', 'errors': record.get('Errors', record.get('_parse_error', 'invalid')),
                'raw_payload': original, 'input_sha256': hashlib.sha256(original.encode()).hexdigest()}
    return {**{key: None if record.get(key) is None else str(record[key]) for key in fields},
            'Errors': json.dumps(evidence, sort_keys=True, separators=(',', ':'), allow_nan=False)}
