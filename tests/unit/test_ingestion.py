"""Real Avro push requests, bounded transport and acknowledgement behavior; no cloud IO."""

import base64
import importlib.util
import io
import json
import hashlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import fastavro
import pytest

ROOT = Path(__file__).resolve().parents[2]
PROJECT = "unit-project"
SOURCE_TIME = "2026-10-20T10:15:30+00:00"
MEASUREMENTS = {
    "cocoa": {"cocoa_price": 6500.25},
    "oil": {"oil_price": 72.5},
    "weather": {"precipitation": 3.5, "soil_moisture": 0.42},
}


@pytest.fixture
def consumer(monkeypatch, tmp_path):
    spec = importlib.util.spec_from_file_location("bounded_ingestion", ROOT / "functions/ingestion/main.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "__file__", str(tmp_path / "main.py"))
    for feed in MEASUREMENTS:
        (tmp_path / f"{feed}_schema.avsc").write_bytes((ROOT / "schemas" / f"{feed}_schema.avsc").read_bytes())
    monkeypatch.setenv("GCP_PROJECT", PROJECT)
    monkeypatch.setenv("INGESTION_ROUTES", json.dumps({
        f"projects/{PROJECT}/subscriptions/{feed}-sub": {"feed": feed, "table": f"{PROJECT}.staging.{feed}"}
        for feed in MEASUREMENTS
    }))
    return module


def envelope(feed="oil", record=None, payload=None):
    record = record or {"date": "2026-10-20", **MEASUREMENTS[feed], "ingestion_time": SOURCE_TIME, "raw_payload": "{\"source\":\"fixture\"}"}
    if payload is None:
        buffer = io.BytesIO()
        schema = json.loads((ROOT / "schemas" / f"{feed}_schema.avsc").read_bytes())
        fastavro.schemaless_writer(buffer, schema, record)
        payload = buffer.getvalue()
    return {"subscription": f"projects/{PROJECT}/subscriptions/{feed}-sub", "message": {
        "messageId": "123456789", "data": base64.b64encode(payload).decode(),
        "attributes": {"googclient_schemaencoding": "BINARY"},
    }}


def request(body, method="POST"):
    return SimpleNamespace(method=method, stream=io.BytesIO(json.dumps(body).encode()))


@pytest.mark.parametrize("feed", MEASUREMENTS)
def test_real_binary_reaches_sink_before_ack(consumer, monkeypatch, feed, capsys):
    events = []
    def write(table, row, identity):
        assert table == f"{PROJECT}.staging.{feed}"
        assert row["date"] == "2026-10-20"
        assert row["raw_payload"] == '{"source":"fixture"}'
        assert all(row[key] == value for key, value in MEASUREMENTS[feed].items())
        assert len(identity) == 64
        assert row["ingestion_time"] == row["producer_acquired_at"] == SOURCE_TIME
        assert row["producer_timestamp_raw"] == SOURCE_TIME
        assert row["consumer_received_at"] != SOURCE_TIME
        assert row["timestamp_status"] == "valid"
        assert row["source_published_at"] is None
        assert row["delivery_id"] == identity and row["pubsub_message_id"] == "123456789"
        assert row["acquisition_id"] is None and row["observation_id"] is None
        events.append("write accepted")
    monkeypatch.setattr(consumer, "insert_row", write)
    assert consumer.stage_analytics_data(request(envelope(feed)))[1] == 204
    assert events == ["write accepted"]
    assert "ingestion_accepted" in capsys.readouterr().out


@pytest.mark.parametrize("feed", MEASUREMENTS)
def test_nullable_wire_fields_are_not_silently_discarded(consumer, feed):
    record = {"date": "2026-10-20", **{k: None for k in MEASUREMENTS[feed]}, "ingestion_time": SOURCE_TIME, "raw_payload": None}
    binary = base64.b64decode(envelope(feed, record)["message"]["data"])
    row = consumer.decode_row(feed, binary, SOURCE_TIME)
    assert all(row[key] is None for key in MEASUREMENTS[feed])
    assert json.loads(row["raw_payload"]) == record


@pytest.mark.parametrize("failure", [TimeoutError("PRIVATE_PAYLOAD"), RuntimeError("PRIVATE_TOKEN")])
def test_sink_failure_never_acknowledged_or_logged_verbatim(consumer, monkeypatch, failure, capsys):
    sink = Mock(side_effect=failure)
    monkeypatch.setattr(consumer, "insert_row", sink)
    assert consumer.stage_analytics_data(request(envelope()))[1] == 503
    sink.assert_called_once()
    logs = capsys.readouterr().out
    assert "ingestion_retry" in logs and "ingestion_accepted" not in logs
    assert "PRIVATE" not in logs


@pytest.mark.parametrize("mutation", ["bad_avro", "trailing_avro", "unknown_route", "no_id", "not_binary", "bad_base64", "bad_json", "oversized", "nonfinite"])
def test_invalid_requests_are_bounded_and_never_dropped_as_success(consumer, monkeypatch, mutation):
    body = envelope()
    if mutation == "bad_avro":
        body = envelope(payload=b"\xff")
    if mutation == "trailing_avro":
        body["message"]["data"] = base64.b64encode(base64.b64decode(body["message"]["data"])+b"extra").decode()
    if mutation == "unknown_route":
        body["subscription"] = "projects/other-project/subscriptions/oil-sub"
    if mutation == "no_id":
        del body["message"]["messageId"]
    if mutation == "not_binary":
        body["message"]["attributes"] = {}
    if mutation == "bad_base64":
        body["message"]["data"] = "!invalid!"
    if mutation == "nonfinite":
        body = envelope(record={"date":"2026-10-20","oil_price":float('nan'),"ingestion_time":SOURCE_TIME,"raw_payload":"safe"})
    req = request(body)
    if mutation == "bad_json":
        req.stream = io.BytesIO(b"not-json")
    if mutation == "oversized":
        monkeypatch.setattr(consumer, "MAX_ENVELOPE_BYTES", 10)
    sink = Mock()
    monkeypatch.setattr(consumer, "insert_row", sink)
    assert consumer.stage_analytics_data(req)[1] == 503
    sink.assert_not_called()


def test_redelivery_keeps_stable_insert_hint_but_different_message_gets_new_identity(consumer, monkeypatch):
    sink = Mock()
    monkeypatch.setattr(consumer, "insert_row", sink)
    body = envelope()
    assert consumer.stage_analytics_data(request(body))[1] == 204
    assert consumer.stage_analytics_data(request(body))[1] == 204
    body["message"]["messageId"] = "123456790"
    assert consumer.stage_analytics_data(request(body))[1] == 204
    ids = [call.args[2] for call in sink.call_args_list]
    assert ids[0] == ids[1] and ids[1] != ids[2]


def test_logical_identities_survive_republication_with_a_new_transport_id(consumer, monkeypatch):
    sink = Mock()
    monkeypatch.setattr(consumer, "insert_row", sink)
    body = envelope()
    revision = hashlib.sha256(json.dumps(['EIA','2026-10-20',MEASUREMENTS['oil']],
        sort_keys=True,separators=(',',':')).encode()).hexdigest()
    body['message']['attributes'].update(acquisition_id='a' * 64, observation_id=revision)
    assert consumer.stage_analytics_data(request(body))[1] == 204
    body['message']['messageId'] = '123456790'
    assert consumer.stage_analytics_data(request(body))[1] == 204
    first, second = [call.args[1] for call in sink.call_args_list]
    assert first['acquisition_id'] == second['acquisition_id'] == 'a' * 64
    assert first['observation_id'] == second['observation_id'] == revision
    assert first['delivery_id'] != second['delivery_id']


def test_invalid_identity_is_not_accepted_or_logged(consumer, monkeypatch, capsys):
    body = envelope()
    body['message']['attributes']['acquisition_id'] = 'private-identity-canary'
    sink = Mock()
    monkeypatch.setattr(consumer, 'insert_row', sink)
    assert consumer.stage_analytics_data(request(body))[1] == 503
    sink.assert_not_called()
    assert 'private-identity-canary' not in capsys.readouterr().out


@pytest.mark.parametrize("result,error", [({},None), ({"insertErrors":[{"index":0,"errors":[{"message":"PRIVATE"}]}]},None), ({},TimeoutError())])
def test_rest_transport_has_bounded_timeout_and_checks_row_errors(consumer, monkeypatch, result, error):
    response = Mock()
    response.raise_for_status.side_effect = error
    response.json.return_value = result
    transport = Mock()
    transport.post.return_value = response
    monkeypatch.setattr(consumer, "session", lambda: transport)
    if error or result.get("insertErrors"):
        with pytest.raises((TimeoutError,RuntimeError)):
            consumer.insert_row(f"{PROJECT}.staging.oil", {"date":"2026-10-20"}, "id")
    else:
        consumer.insert_row(f"{PROJECT}.staging.oil", {"date":"2026-10-20"}, "id")
    kwargs = transport.post.call_args.kwargs
    assert kwargs['timeout'] == (3.05,20) and kwargs['max_allowed_time'] == 30
    assert kwargs['json']['skipInvalidRows'] is False and kwargs['json']['ignoreUnknownValues'] is False


def test_non_post_never_consumes_input(consumer):
    req = request({},method="HEAD")
    assert consumer.stage_analytics_data(req)[1] == 405
    assert req.stream.tell() == 0


@pytest.mark.parametrize("feed", MEASUREMENTS)
@pytest.mark.parametrize("stamp,normalized,status", [
    ("2026-10-20T12:15:30+02:00", SOURCE_TIME, "valid"),
    ("2026-10-20T03:15:30-07:00", SOURCE_TIME, "valid"),
    ("2026-10-20T10:15:30Z", SOURCE_TIME, "valid"),
    (None, None, "missing"),
    ("", None, "invalid"),
    ("2026-10-20", None, "invalid"),
    ("2026-02-30T10:15:30Z", None, "invalid"),
    ("2026-10-20T10:15:30+25:00", None, "invalid"),
    ("PRIVATE_INVALID_TIMESTAMP", None, "invalid"),
    ("2099-01-01T00:00:00Z", None, "future"),
])
def test_timestamp_provenance_survives_real_avro_and_replay(consumer, feed, stamp, normalized, status):
    record = {"date": "2026-10-20", **MEASUREMENTS[feed], "ingestion_time": stamp, "raw_payload": None}
    binary = base64.b64decode(envelope(feed, record)["message"]["data"])
    first = consumer.decode_row(feed, binary, "2026-10-27T10:00:00+00:00")
    replay = consumer.decode_row(feed, binary, "2026-10-28T10:00:00+00:00")
    for row in (first, replay):
        assert row["ingestion_time"] == row["producer_acquired_at"] == normalized
        assert row["producer_timestamp_raw"] == stamp
        assert row["timestamp_status"] == status
        assert row["source_published_at"] is None
        assert json.loads(row["raw_payload"]) == record
    assert first["consumer_received_at"] != replay["consumer_received_at"]


@pytest.mark.parametrize("feed", MEASUREMENTS)
def test_naive_time_is_only_normalized_for_the_verified_legacy_weather_writer(consumer, feed):
    value = "2026-10-20T10:15:30"
    row = consumer.timestamp_fields(feed, value, "2026-10-27T10:00:00+00:00")
    assert row["producer_timestamp_raw"] == value
    assert row["producer_acquired_at"] == (SOURCE_TIME if feed == "weather" else None)
    assert row["timestamp_status"] == ("legacy_naive_utc" if feed == "weather" else "invalid")


@pytest.mark.parametrize("stamp,status", [
    ("2026-10-27T10:05:00Z", "valid"),
    ("2026-10-27T10:05:00.000001Z", "future"),
    ("2026-10-27T12:05:00+02:00", "valid"),
])
def test_future_clock_skew_is_bounded_after_utc_normalization(consumer, stamp, status):
    row = consumer.timestamp_fields("oil", stamp, "2026-10-27T10:00:00+00:00")
    assert row["timestamp_status"] == status
    assert (row["producer_acquired_at"] is None) == (status == "future")


def test_invalid_timestamp_is_flagged_without_logging_its_contents(consumer, monkeypatch, capsys):
    record = {"date": "2026-10-20", "oil_price": 72.5, "ingestion_time": "PRIVATE_INVALID_TIMESTAMP", "raw_payload": None}
    sink = Mock()
    monkeypatch.setattr(consumer, "insert_row", sink)
    assert consumer.stage_analytics_data(request(envelope(record=record)))[1] == 204
    row = sink.call_args.args[1]
    assert row["ingestion_time"] is None and row["producer_acquired_at"] is None
    logs = capsys.readouterr().out
    assert '"timestamp_status": "invalid"' in logs
    assert "PRIVATE_INVALID_TIMESTAMP" not in logs
