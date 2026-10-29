"""Coverage commits follow complete source reconciliation and all publish ACKs."""

import sys
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from test_oil_logging import oil_handler as oil_fixture, page, row
from test_producer_http_failures import handler as adapter_fixture, invoke
from test_weather_timestamps import weather as weather_fixture


@pytest.fixture
def oil(monkeypatch, tmp_path):
    return oil_fixture.__wrapped__(monkeypatch, tmp_path)


@pytest.fixture
def weather(monkeypatch, tmp_path):
    return weather_fixture.__wrapped__(monkeypatch, tmp_path)


@pytest.fixture
def adapter(monkeypatch, tmp_path):
    return adapter_fixture.__wrapped__(monkeypatch, tmp_path)


def test_eia_recovery_window_and_older_revision_are_published_before_commit(oil, monkeypatch):
    progress = SimpleNamespace(start=date(2026, 9, 20), end=date(2026, 10, 27), complete=Mock(return_value={}))
    monkeypatch.setattr(oil.module, 'coverage', lambda *args, **kwargs: progress)
    oil.requests.get.return_value = page([row('2026-10-01', '71.5'), row('2026-10-20', '72')])

    def complete(receipt, rows):
        assert oil.publisher.publish.return_value.result.call_count == 2
        oil.module.verify_against_published.assert_called_once()
        assert rows[0] == {'date': '2026-10-01', 'oil_price': 71.5}
        return {'coverage_stage': 'publish_acknowledged'}

    progress.complete.side_effect = complete
    assert oil.module.publish_eia_data()['coverage_stage'] == 'publish_acknowledged'
    assert oil.requests.get.call_args.kwargs['params']['start'] == '2026-09-20'


@pytest.mark.parametrize('failure', ['source_disagreement', 'partial_publish'])
def test_eia_failed_or_partial_collection_does_not_commit(oil, monkeypatch, failure):
    progress = SimpleNamespace(start=date(2026, 9, 20), end=date(2026, 10, 27), complete=Mock())
    monkeypatch.setattr(oil.module, 'coverage', lambda *args, **kwargs: progress)
    oil.requests.get.return_value = page([row('2026-10-01'), row('2026-10-20')])
    if failure == 'source_disagreement':
        oil.module.verify_against_published.side_effect = RuntimeError('EIA API and published observations disagree')
    else:
        oil.publisher.publish.return_value.result.side_effect = [None, RuntimeError('publish failed')]
    with pytest.raises(RuntimeError):
        oil.module.publish_eia_data()
    progress.complete.assert_not_called()


def test_weather_missing_calendar_date_fails_before_any_publish(weather, monkeypatch):
    progress = SimpleNamespace(start=date(2026, 10, 26), end=date(2026, 10, 28), complete=Mock())
    monkeypatch.setattr(weather.module, 'coverage', lambda *args, **kwargs: progress)
    with pytest.raises(RuntimeError, match='Invalid NASA'):
        weather.module.publish_weather_data()
    weather.publisher.publish.assert_not_called()
    progress.complete.assert_not_called()


def test_weather_explicit_source_missing_is_preserved_in_complete_window(weather, monkeypatch):
    progress = SimpleNamespace(start=date(2026, 10, 26), end=date(2026, 10, 28), complete=Mock(return_value={}))
    monkeypatch.setattr(weather.module, 'coverage', lambda *args, **kwargs: progress)
    for values in weather.data['properties']['parameter'].values():
        values['20261028'] = -999
    weather.module.publish_weather_data()
    record, rows = progress.complete.call_args.args
    assert record['published'] == weather.publisher.publish.return_value.result.call_count == 3
    assert rows[-1] == {'date': '2026-10-28', 'precipitation': None, 'soil_moisture': None}


@pytest.mark.parametrize('feed', ['cocoa', 'oil', 'weather'])
def test_coverage_persistence_failure_is_http_500_without_completion_receipt(adapter, monkeypatch, capsys, feed):
    client = adapter.create(feed)
    module = sys.modules['main']
    progress = SimpleNamespace(start=date(2026, 10, 26), end=date(2026, 10, 28),
                               complete=Mock(side_effect=RuntimeError('Acquisition coverage persistence failed')))
    monkeypatch.setattr(module, 'coverage', lambda *args, **kwargs: progress)
    if feed == 'cocoa':
        monkeypatch.setattr(module, 'fetch_window', lambda *args, **kwargs: [(date(2026, 10, 26), 5075.02)])
    if feed == 'weather':
        original = module.requests.get.side_effect

        def get(*args, **kwargs):
            response = original(*args, **kwargs)
            data = response.json()
            for values in data['properties']['parameter'].values():
                values['20261028'] = -999
            return SimpleNamespace(status_code=200, json=lambda: data)

        module.requests.get.side_effect = get
    assert invoke(client, feed).status_code == 500
    progress.complete.assert_called_once()
    assert 'acquisition_complete' not in capsys.readouterr().out
