"""The real weather writer emits one explicit UTC acquisition per response."""

import importlib.util
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import fastavro
import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def weather(monkeypatch, tmp_path):
    google, cloud, pubsub = (ModuleType(name) for name in ("google", "google.cloud", "google.cloud.pubsub_v1"))
    google.cloud = cloud
    cloud.pubsub_v1 = pubsub
    publisher = Mock()
    publisher.topic_path.return_value = "projects/unit/topics/weather"
    pubsub.PublisherClient = Mock(return_value=publisher)
    data = {"header": {"time_standard": "LST"}, "properties": {"parameter": {
        "PRECTOTCORR": {"20261026": 2.5, "20261027": -999.0},
        "GWETROOT": {"20261026": 0.42, "20261027": 0.41},
    }}}
    requests = ModuleType("requests")
    requests.RequestException = TimeoutError
    requests.get = Mock(return_value=SimpleNamespace(status_code=200, json=lambda: data))
    for module in (google, cloud, pubsub, requests):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    (tmp_path / "request_policy.py").write_bytes((ROOT / "functions/shared/request_policy.py").read_bytes())
    monkeypatch.syspath_prepend(str(tmp_path))
    for target, source in (("main.py", "functions/weather/main.py"), ("weather_schema.avsc", "schemas/weather_schema.avsc")):
        (tmp_path / target).write_bytes((ROOT / source).read_bytes())
    spec = importlib.util.spec_from_file_location("weather_time_under_test", tmp_path / "main.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            assert tz is timezone.utc
            return datetime(2026, 10, 28, 10, tzinfo=timezone.utc)

    module.datetime = Clock
    return SimpleNamespace(module=module, requests=requests, publisher=publisher, data=data)


def test_weather_batch_preserves_observation_dates_and_uses_one_aware_acquisition(weather):
    module, publisher, data = weather.module, weather.publisher, weather.data
    module.publish_weather_data()
    schema = json.loads((ROOT / "schemas/weather_schema.avsc").read_bytes())
    rows = [fastavro.schemaless_reader(io.BytesIO(call.args[1]), schema) for call in publisher.publish.call_args_list]
    assert [r["date"] for r in rows] == ["2026-10-26", "2026-10-27"]
    assert {r["ingestion_time"] for r in rows} == {"2026-10-28T10:00:00+00:00"}
    assert all(json.loads(r["raw_payload"]) == data for r in rows)
    assert rows[1]["precipitation"] is None
    publisher.publish.return_value.result.assert_called_with(timeout=10)


def test_partial_weather_publication_fails_and_does_not_swallow_timeout(weather):
    weather.publisher.publish.return_value.result.side_effect = ["id", TimeoutError("private-sdk-canary")]
    with pytest.raises(RuntimeError, match="batch incomplete") as error:
        weather.module.publish_weather_data()
    assert weather.publisher.publish.call_count == 2
    assert error.value.__suppress_context__
    assert "private-sdk-canary" not in str(error.value)


def test_weather_http_failure_does_not_publish_or_log_body(weather, capsys):
    weather.requests.get.return_value = SimpleNamespace(status_code=403, text="private-body-canary")
    with pytest.raises(RuntimeError, match="HTTP status 403"):
        weather.module.publish_weather_data()
    weather.publisher.publish.assert_not_called()
    assert "private-body-canary" not in capsys.readouterr().out


def test_invalid_weather_response_fails_without_logging_body(weather, capsys):
    weather.requests.get.return_value = SimpleNamespace(status_code=200, json=lambda: {"error": "private-body-canary"})
    with pytest.raises(RuntimeError, match="Invalid NASA POWER response"):
        weather.module.publish_weather_data()
    weather.publisher.publish.assert_not_called()
    assert "private-body-canary" not in capsys.readouterr().out


@pytest.mark.parametrize("precips,moistures", [
    ({}, {}),
    ([], []),
    ({"20261026": 1}, {"20261027": 0.4}),
    ({"20261026": 1, "20261027": "private-body-canary"}, {"20261026": 0.4, "20261027": 0.3}),
    ({"20261026": float('nan')}, {"20261026": 0.4}),
    ({"20261026": float('inf')}, {"20261026": 0.4}),
    ({"20261026": True}, {"20261026": 0.4}),
    ({"20261029": 1}, {"20261029": 0.4}),
    ({"20261001": 1}, {"20261001": 0.4}),
    ({"2026102": 1}, {"2026102": 0.4}),
])
def test_invalid_window_is_rejected_before_any_publication(weather, capsys, precips, moistures):
    weather.data['properties']['parameter'] = {'PRECTOTCORR': precips, 'GWETROOT': moistures}
    with pytest.raises(RuntimeError, match="Invalid NASA POWER response"):
        weather.module.publish_weather_data()
    weather.publisher.publish.assert_not_called()
    output = capsys.readouterr().out
    assert 'acquisition_complete' not in output and 'private-body-canary' not in output


def test_publisher_credentials_failure_has_no_success_receipt(weather, capsys):
    weather.module.pubsub_v1.PublisherClient.side_effect = RuntimeError('private-credential-canary')
    with pytest.raises(RuntimeError, match="publisher initialization failed"):
        weather.module.publish_weather_data()
    output = capsys.readouterr().out
    assert 'acquisition_complete' not in output and 'private-credential-canary' not in output
