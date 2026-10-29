"""Historical malformed/missing inputs reach durable invalid output, never a filter drop."""

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

from test_eia_backfill import cleaner as oil_fixture

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def cleaners(monkeypatch):
    oil = oil_fixture.__wrapped__(monkeypatch)
    result = {'oil': oil}
    for feed, filename in [('cocoa', 'clean_cocoa_prices.py'), ('weather', 'clean_precipitation.py')]:
        spec = importlib.util.spec_from_file_location('backfill_'+feed, ROOT/'pipelines/backfill'/filename)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        result[feed] = module
    return result


@pytest.mark.parametrize('feed,line', [('cocoa', 'PRIVATE malformed'), ('weather', 'PRIVATE malformed'),
                                       ('weather', '2026,367,2.5,0.4'), ('weather', '2026,0,2.5,0.4')])
def test_bad_csv_retains_exact_original_line_and_source_reference(cleaners, capsys, feed, line):
    module = cleaners[feed]
    record = module.parse_csv(line)
    if feed == 'weather':
        record = module.clean_and_transform(record)
        assert module.filter_missing_data(record)
    tagged = list(module.ValidateAndTransform().process(record))
    assert len(tagged) == 1 and tagged[0][0] == 'invalid'
    stored = module.invalid_record(tagged[0][1], feed)
    evidence = json.loads(stored['Errors'])
    assert evidence['raw_payload'] == line
    assert evidence['input_sha256'] == hashlib.sha256(line.encode()).hexdigest()
    assert evidence['source_uri'].startswith('gs://raw-historic-data/snapshots/')
    assert evidence['source_acquired_at'] is None and evidence['rejected_at']
    assert 'PRIVATE' not in capsys.readouterr().out


@pytest.mark.parametrize('line', ['2026,20,-999,0.4', '2026,20,-999.0,0.4', '2026,20,2.5,-999',
                                 '2026,20,nan,0.4', '2026,20,2.5,1.5'])
def test_weather_missing_or_invalid_measurements_reach_invalid_sink(cleaners, line):
    module = cleaners['weather']
    parsed = module.clean_and_transform(module.parse_csv(line))
    assert module.filter_missing_data(parsed)
    output = list(module.ValidateAndTransform().process(parsed))
    assert len(output) == 1 and output[0][0] == 'invalid'
    stored = module.invalid_record(output[0][1], 'weather')
    assert json.loads(stored['Errors'])['raw_payload'] == line
    assert set(stored) == {'date', 'precipitation', 'soil_moisture', 'Errors'}
    assert all(value is None or isinstance(value, str) for value in stored.values())


@pytest.mark.parametrize('price', ['nan', 'inf', '0', '-1', 'not-a-price'])
def test_cocoa_bad_price_is_recorded_with_original_source_value(cleaners, price):
    module = cleaners['cocoa']
    output = list(module.ValidateAndTransform().process({'Date': '20/10/2026', 'Euro_Price': price}))
    assert len(output) == 1 and output[0][0] == 'invalid'
    saved = module.invalid_record(output[0][1], 'cocoa')
    assert json.loads(json.loads(saved['Errors'])['raw_payload'])['Euro_Price'] == price


def test_invalid_eia_source_value_is_retained_before_numeric_conversion(cleaners):
    module = cleaners['oil']
    observation = {'period': '2026-10-20', 'value': 'PRIVATE bad number', 'series': 'RBRTE', 'units': '$/BBL'}
    archive = {'series': 'RBRTE', 'units': '$/BBL', 'acquisition_interface': 'published-table', 'response': {'data': [observation]}}
    extracted = list(module.extract_and_clean(archive))[0]
    output = list(module.ValidateAndTransform().process(extracted))
    saved = module.invalid_record(output[0][1], 'oil')
    assert json.loads(json.loads(saved['Errors'])['raw_payload']) == observation
