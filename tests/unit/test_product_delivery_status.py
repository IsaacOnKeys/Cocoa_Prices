"""Exercise the view's decision logic locally; no cloud queries or model training."""

import re
import runpy
import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
RENDER = runpy.run_path(str(ROOT / 'scripts/deploy/render_delivery_sql.py'))['render']


def status(local_now, accepted_date, registry_status='published', marker=True):
    sql = RENDER('product_status_view').split(' AS\n', 1)[1]
    sql = sql.replace("CURRENT_DATETIME('Europe/Berlin')", ':local_now')
    sql = sql.replace('CURRENT_TIMESTAMP()', ':checked_at')
    sql = sql.replace("TIME '20:00:00'", "'20:00:00'")
    sql = re.sub(r'EXTRACT\(DAYOFWEEK FROM (\w+)\)', r"(CAST(STRFTIME('%w',\1) AS INTEGER)+1)", sql)
    sql = re.sub(r'DATE_SUB\((DATE\(local_now\)|candidate_date), INTERVAL (\d) DAY\)',
                 r"DATE(\1,'-\2 day')", sql)
    sql = sql.replace('`cocoa-prices-430315.analytics_delivery.active_release`', 'active_release')
    sql = sql.replace('`cocoa-prices-430315.analytics_delivery.release_runs`', 'release_runs')
    sql = sql.replace("UNNEST(['core_analytics','forecasts','bi_serving']) AS product",
                      "(SELECT 'core_analytics' AS product UNION ALL SELECT 'forecasts' UNION ALL SELECT 'bi_serving')")
    with sqlite3.connect(':memory:') as db:
        db.row_factory = sqlite3.Row
        db.executescript('''
            CREATE TABLE active_release(singleton,release_id,business_date,published_at);
            CREATE TABLE release_runs(release_id,business_date,published_at,input_cutoff,model_id,status);
        ''')
        if marker:
            db.execute('INSERT INTO active_release VALUES(1,?,?,?)',
                       ('accepted', accepted_date, '2026-10-23T17:12:00Z'))
        if accepted_date is not None:
            db.execute('INSERT INTO release_runs VALUES(?,?,?,?,?,?)',
                       ('accepted', accepted_date, '2026-10-23T17:10:00Z',
                        '2026-10-23T17:05:00Z', 'retained_model', registry_status))
        return [dict(row) for row in db.execute(sql, {'local_now': local_now, 'checked_at': 'query_time'})]


@pytest.mark.parametrize('now,accepted,expected,outcome', [
    ('2026-10-26 19:59:59', '2026-10-23', '2026-10-23', 'current'),  # Monday
    ('2026-10-26 20:00:00', '2026-10-23', '2026-10-26', 'missed_update'),
    ('2026-10-26 20:00:00', '2026-10-26', '2026-10-26', 'current'),
    ('2026-10-27 00:00:00', '2026-10-26', '2026-10-26', 'current'),
    ('2026-10-27 19:00:00', '2026-10-27', '2026-10-26', 'current'),  # Early success
    ('2026-10-30 20:00:00', '2026-10-29', '2026-10-30', 'missed_update'),
    ('2026-10-24 00:00:00', '2026-10-23', '2026-10-23', 'current'),
    ('2026-10-25 23:59:59', '2026-10-23', '2026-10-23', 'current'),  # DST weekend
    ('2026-10-25 23:59:59', '2026-10-22', '2026-10-23', 'missed_update'),
    ('2026-03-29 03:00:00', '2026-03-27', '2026-03-27', 'current'),  # Spring weekend
    ('2026-12-25 20:00:00', '2026-12-24', '2026-12-25', 'missed_update'),  # Holiday
    ('2026-10-26 20:00:00', '2026-10-27', '2026-10-26', 'unavailable'),
])
def test_delivery_calendar_and_shared_policy(now, accepted, expected, outcome):
    rows = status(now, accepted)
    assert {row['product'] for row in rows} == {'core_analytics', 'forecasts', 'bi_serving'}
    assert len(rows) == 3
    assert all(row['expected_business_date'] == expected and row['delivery_status'] == outcome for row in rows)
    assert all(row['accepted_business_date'] == accepted for row in rows)


@pytest.mark.parametrize('accepted,registry_status,marker', [
    (None, 'published', False), (None, 'published', True),
    ('2026-10-23', 'failed', True), ('2026-10-23', 'validated', True),
])
def test_missing_or_unpublished_release_is_unavailable(accepted, registry_status, marker):
    rows = status('2026-10-26 20:00:00', accepted, registry_status, marker)
    assert all(row['delivery_status'] == 'unavailable' and row['release_id'] is None for row in rows)


def test_overdue_release_preserves_identity_model_and_original_times():
    rows = status('2026-10-26 20:00:00', '2026-10-23')
    for row in rows:
        assert row['delivery_status'] == 'missed_update'
        assert row['release_id'] == 'accepted' and row['model_id'] == 'retained_model'
        assert row['original_publication_time'] == '2026-10-23T17:10:00Z'
        assert row['activated_at'] == '2026-10-23T17:12:00Z'
        assert row['input_cutoff'] == '2026-10-23T17:05:00Z'
        assert 'previous accepted release' in row['status_message']
