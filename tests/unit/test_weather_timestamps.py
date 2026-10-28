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

ROOT = Path(__file__).resolve().parents[2]


def test_weather_batch_preserves_observation_dates_and_uses_one_aware_acquisition(monkeypatch, tmp_path):
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
    requests.get = Mock(return_value=SimpleNamespace(status_code=200, json=lambda: data))
    for module in (google, cloud, pubsub, requests):
        monkeypatch.setitem(sys.modules, module.__name__, module)
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
    module.publish_weather_data()
    schema = json.loads((ROOT / "schemas/weather_schema.avsc").read_bytes())
    rows = [fastavro.schemaless_reader(io.BytesIO(call.args[1]), schema) for call in publisher.publish.call_args_list]
    assert [r["date"] for r in rows] == ["2026-10-26", "2026-10-27"]
    assert {r["ingestion_time"] for r in rows} == {"2026-10-28T10:00:00+00:00"}
    assert all(json.loads(r["raw_payload"]) == data for r in rows)
    assert rows[1]["precipitation"] is None
