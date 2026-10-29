"""Exercise the public-history parser and multi-observation producer path."""

import io
import json
import time
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

import fastavro
import pytest

from test_cocoa_dates import ROOT, html
from test_cocoa_dates import cocoa as cocoa_fixture


@pytest.fixture
def cocoa(monkeypatch, tmp_path):
    return cocoa_fixture.__wrapped__(monkeypatch, tmp_path)


def page():
    config = {'tableWpId': 26, 'dataTableParams': {'ajax': {'url': 'https://www.icco.org/wp-admin/admin-ajax.php?action=get_wdtable&table_id=26'},
                                               'aLengthMenu': [[10, 25, 100]]}}
    return ('<input id="table_1_desc" value=\''+json.dumps(config)+'\'>'
            '<input id="wdtNonceFrontendServerSide_26" value="unit-nonce">'
            '<table id="table_1"><thead><tr><th>ID</th><th>Date</th><th>London</th><th>NY</th><th>USD</th>'
            '<th>ICCO daily price (Euro/tonne)</th></tr></thead></table>')


def setup(cocoa, rows=None):
    rows = rows if rows is not None else [
        ['4', '27/10/2026', '', '', '', '5,121.21'],
        ['3', '10/10/2026', '', '', '', '5,200.00'],
        ['2', '28/09/2026', '', '', '', '5,100.00'],
    ]
    cocoa.requests.get.side_effect = lambda url, **kw: SimpleNamespace(
        status_code=200, text=page() if '/statistics/' in url else html([('27/10/2026', '5,121.21')]))
    cocoa.requests.post = Mock(return_value=SimpleNamespace(status_code=200, json=lambda: {
        'recordsTotal': str(len(rows)), 'recordsFiltered': str(len(rows)), 'data': rows}))
    return rows


def test_older_correction_and_latest_quote_are_both_acquired(cocoa):
    setup(cocoa)
    observations = cocoa.module.fetch_window(date(2026, 9, 29), date(2026, 10, 28), deadline=time.monotonic()+40)
    assert observations == [(date(2026, 10, 10), 5200), (date(2026, 10, 27), 5121.21)]
    request = cocoa.requests.post.call_args.kwargs
    assert request['data']['length'] == 100 and request['data']['order[0][dir]'] == 'desc'


@pytest.mark.parametrize('rows', [
    [['1', '27/10/2026', '', '', '', '5000']],  # requested boundary no longer exposed
    [['2', '27/10/2026', '', '', '', '5000'], ['1', '27/10/2026', '', '', '', '5001']],
    [['2', '10/10/2026', '', '', '', '5000'], ['1', '27/10/2026', '', '', '', '5001']],
    [['2', '27/10/2026', '', '', '', 'NaN'], ['1', '01/09/2026', '', '', '', '5000']],
    [['2', '29/10/2026', '', '', '', '5000'], ['1', '01/09/2026', '', '', '', '5000']],
])
def test_incomplete_duplicate_unordered_or_invalid_history_fails(cocoa, rows):
    setup(cocoa, rows)
    with pytest.raises(RuntimeError, match='incomplete ICCO'):
        cocoa.module.fetch_window(date(2026, 9, 29), date(2026, 10, 28), deadline=time.monotonic()+40)


def test_disagreeing_publisher_interfaces_fail(cocoa):
    rows = setup(cocoa)
    rows[0][-1] = '5,000'
    with pytest.raises(RuntimeError, match='incomplete ICCO'):
        cocoa.module.fetch_window(date(2026, 9, 29), date(2026, 10, 28), deadline=time.monotonic()+40)


def test_all_homepage_quotes_are_included_while_statistics_lags(cocoa):
    setup(cocoa)
    cocoa.requests.get.side_effect = lambda url, **kw: SimpleNamespace(
        status_code=200, text=page() if '/statistics/' in url else html([
            ('27/10/2026', '5,121.21'), ('26/10/2026', '5,034.38')]))
    observations = cocoa.module.fetch_window(date(2026, 9, 29), date(2026, 10, 28), deadline=time.monotonic()+40)
    assert (date(2026, 10, 26), 5034.38) in observations


def test_multi_row_acknowledgements_precede_coverage_commit(cocoa, monkeypatch):
    setup(cocoa)
    progress = SimpleNamespace(start=date(2026, 9, 29), end=date(2026, 10, 28), complete=Mock(return_value={}))
    monkeypatch.setattr(cocoa.module, 'coverage', lambda *args, **kwargs: progress)

    def complete(receipt, rows):
        assert cocoa.publisher.publish.return_value.result.call_count == 2
        assert receipt['published'] == 2 and [r['date'] for r in rows] == ['2026-10-10', '2026-10-27']
        return {'coverage_stage': 'publish_acknowledged'}

    progress.complete.side_effect = complete
    assert cocoa.module.publish_price(None)[1] == 200
    progress.complete.assert_called_once()
    schema = json.loads((ROOT/'schemas/cocoa_schema.avsc').read_bytes())
    decoded = [fastavro.schemaless_reader(io.BytesIO(c.args[1]), schema) for c in cocoa.publisher.publish.call_args_list]
    assert [r['date'] for r in decoded] == ['2026-10-10', '2026-10-27']
    assert len({r['ingestion_time'] for r in decoded}) == 1


def test_partial_batch_does_not_commit_coverage(cocoa, monkeypatch, capsys):
    setup(cocoa)
    progress = SimpleNamespace(start=date(2026, 9, 29), end=date(2026, 10, 28), complete=Mock())
    monkeypatch.setattr(cocoa.module, 'coverage', lambda *args, **kwargs: progress)
    cocoa.publisher.publish.return_value.result.side_effect = [None, RuntimeError('private-sdk-canary')]
    assert cocoa.module.publish_price(None)[1] == 500
    progress.complete.assert_not_called()
    output = capsys.readouterr().out
    assert 'acquisition_complete' not in output and 'private-sdk-canary' not in output
