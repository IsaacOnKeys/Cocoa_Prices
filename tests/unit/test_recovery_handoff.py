"""Archive -> replay -> real push handler -> release grouping, without cloud IO."""

import base64
import re
import sqlite3
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from test_daily_raw_archive import ACQUIRED, REPLAY, ROOT, documents
from test_daily_raw_archive import archive as archive
from test_ingestion import consumer as consumer
from test_ingestion import request


def frozen_oil(rows):
    """Execute the production SELECT; adapt only parameters and SQL dialect."""
    sql = (ROOT / 'sql/procedures/run_analytics_release.sql').read_text()
    query = re.search(r"SELECT p_release_id,p_business_date,'stage','oil'.*?GROUP BY[^\n]+", sql, re.S).group()
    query = query.replace('`${project}.${staging}.oil_prices`', 'stage')
    query = query.replace("SAFE_CAST(JSON_VALUE(p_sources,'$.oil.acquired_at') AS TIMESTAMP)", ':acquired')
    query = query.replace("JSON_VALUE(p_sources,'$.oil.acquisition_id')", ':acquisition')
    for name in ('p_release_id', 'p_business_date', 'input_cutoff'):
        query = query.replace(name, ':' + name)
    with sqlite3.connect(':memory:') as db:
        columns = ('date', 'oil_price', 'acquisition_id', 'observation_id', 'producer_acquired_at',
                   'consumer_received_at', 'source_published_at', 'timestamp_status')
        db.execute('CREATE TABLE stage (' + ','.join(columns) + ')')
        db.executemany('INSERT INTO stage VALUES (?,?,?,?,?,?,?,?)',
                       [tuple(row[k] for k in columns) for row in rows])
        return db.execute(query, {'p_release_id': 'recovery', 'p_business_date': '2026-11-06',
                                 'input_cutoff': '2026-11-06T23:59:59+00:00',
                                 'acquired': ACQUIRED, 'acquisition': 'a' * 64}).fetchall()


def test_crash_restart_and_expired_transport_reconcile_raw(archive, consumer, monkeypatch):  # noqa: F811 - imported pytest fixtures
    cloud, _, records, publish = archive
    receipt = publish()
    data = documents(cloud, 'batches')[0][2]
    batch, decoded = REPLAY['verified_batch'](data, receipt['raw_input']['sha256'])
    assert decoded == records
    sink, deliveries = [], []

    def insert(table, row, identity):
        # Deliberately provide NO insertId deduplication: BigQuery offers best effort only.
        sink.append(dict(row))
        deliveries.append(identity)

    monkeypatch.setattr(consumer, 'insert_row', insert)

    def push(message, number):
        return request({'subscription': 'projects/unit-project/subscriptions/oil-sub', 'message': {
            'messageId': str(number), 'data': message['data'], 'attributes': message['attributes']}})

    def crash_after_write(event, started, **fields):
        if event == 'ingestion_accepted':
            raise SystemExit('simulated process death before HTTP acknowledgement')

    monkeypatch.setattr(consumer, 'emit', crash_after_write)
    with pytest.raises(SystemExit):
        consumer.stage_analytics_data(push(batch['messages'][0], 100))
    assert len(sink) == 1
    monkeypatch.setattr(consumer, 'emit', Mock())
    # Restart: first message is redelivered; the unfinished second record follows.
    for number, message in enumerate(batch['messages'], 100):
        assert consumer.stage_analytics_data(push(message, number))[1] == 204
    assert len(sink) == 3 and deliveries[0] == deliveries[1]

    class RecoveryClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 6, 12, tzinfo=timezone.utc)

    monkeypatch.setattr(consumer, 'datetime', RecoveryClock)
    # Eight days later: model an empty/expired transport, not an elapsed cloud test.
    transport = []
    assert (RecoveryClock.now() - datetime.fromisoformat(ACQUIRED)).days == 8

    def enqueue(topic, payload, **attributes):
        transport.append({'data': base64.b64encode(payload).decode(), 'attributes': attributes})
        return Mock()

    publisher = Mock()
    publisher.publish.side_effect = enqueue
    assert REPLAY['replay'](batch, publisher, 'projects/unit-project/topics/recovery-topic', 'recovery-drill') == 2
    for number, message in enumerate(transport, 200):
        assert consumer.stage_analytics_data(push(message, number))[1] == 204
    assert len(sink) == 5 and len(set(deliveries)) == 4
    assert all(row['producer_acquired_at'] == ACQUIRED for row in sink)
    assert sink[-1]['consumer_received_at'].startswith('2026-11-06')
    frozen = frozen_oil(sink)
    assert len(frozen) == len(records) == 2
    assert {(row[4], row[5]) for row in frozen} == {(r['date'], r['oil_price']) for r in records}
    assert {row[9] for row in frozen} == {m['attributes']['observation_id'] for m in batch['messages']}
    # Recovery does not manufacture today's acquisition; the existing provenance gate rejects it.
    assert all(datetime.fromisoformat(row['producer_acquired_at']) < RecoveryClock.now() for row in sink)
