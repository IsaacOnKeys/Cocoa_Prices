"""Exercise the packaged oil handler with fake HTTP, Secret Manager and Pub/Sub."""

import datetime
import importlib.util
import io
import json
import sys
import traceback
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import fastavro
import pytest

ROOT = Path(__file__).resolve().parents[2]
CANARY = "test-only-fred-key-must-never-appear-in-output"


@pytest.fixture
def oil_handler(monkeypatch, tmp_path):
    google = ModuleType("google")
    google.__path__ = []
    cloud = ModuleType("google.cloud")
    cloud.__path__ = []
    google.cloud = cloud
    cloud.pubsub_v1 = ModuleType("google.cloud.pubsub_v1")
    cloud.secretmanager = ModuleType("google.cloud.secretmanager")
    cloud.secretmanager.SecretManagerServiceClient = MagicMock(
        side_effect=AssertionError("Real secret lookup is forbidden")
    )
    publisher = MagicMock()
    publisher.topic_path.side_effect = lambda project, topic: f"projects/{project}/topics/{topic}"
    cloud.pubsub_v1.PublisherClient = MagicMock(return_value=publisher)
    requests = ModuleType("requests")

    class RequestException(Exception):
        pass

    requests.RequestException = RequestException
    requests.get = MagicMock()
    for module in (google, cloud, cloud.pubsub_v1, cloud.secretmanager, requests):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setenv("GCP_PROJECT", "unit-test-project")
    monkeypatch.delenv("FRED_OIL_API_KEY", raising=False)
    # Execute from the same complete runtime shape used for deployment.
    runtime = tmp_path / "oil"
    runtime.mkdir()
    for source, target in (
        (ROOT / "functions/oil/main.py", "main.py"),
        (ROOT / "schemas/oil_schema.avsc", "oil_schema.avsc"),
    ):
        (runtime / target).write_bytes(source.read_bytes())
    spec = importlib.util.spec_from_file_location("oil_handler_under_test", runtime / "main.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.get_secret = MagicMock(return_value=CANARY)
    return SimpleNamespace(module=module, requests=requests, publisher=publisher)


def test_success_keeps_key_in_http_request_but_out_of_logs(oil_handler, capsys):
    observation = {"date": "2026-10-20", "value": "72.50"}
    oil_handler.requests.get.return_value = SimpleNamespace(
        status_code=200, json=lambda: {"observations": [observation]}
    )
    oil_handler.module.publish_fred_data()
    captured = capsys.readouterr()
    assert CANARY not in captured.out + captured.err
    args, kwargs = oil_handler.requests.get.call_args
    assert args == ("https://api.stlouisfed.org/fred/series/observations",)
    assert kwargs["params"]["api_key"] == CANARY
    assert kwargs["params"]["series_id"] == "DCOILBRENTEU"
    start = datetime.date.fromisoformat(kwargs["params"]["observation_start"])
    end = datetime.date.fromisoformat(kwargs["params"]["observation_end"])
    assert (end - start).days == 14
    publish_args, publish_kwargs = oil_handler.publisher.publish.call_args
    assert publish_args[0] == "projects/unit-test-project/topics/oil-prices-topic"
    schema = fastavro.parse_schema(json.loads((ROOT / "schemas/oil_schema.avsc").read_bytes()))
    record = fastavro.schemaless_reader(io.BytesIO(publish_args[1]), schema)
    assert record["date"] == observation["date"]
    assert record["oil_price"] == 72.5
    assert json.loads(record["raw_payload"]) == observation
    assert publish_kwargs == {"googclient_schemaencoding": "BINARY"}


def test_http_error_body_is_not_logged(oil_handler, capsys):
    oil_handler.requests.get.return_value = SimpleNamespace(
        status_code=403, text=f"Request rejected: api_key={CANARY}"
    )
    oil_handler.module.publish_fred_data()
    output = capsys.readouterr()
    assert CANARY not in output.out + output.err
    assert "FRED API error 403" in output.out
    oil_handler.publisher.publish.assert_not_called()


def test_request_failure_traceback_does_not_expose_key(oil_handler, capsys):
    oil_handler.requests.get.side_effect = oil_handler.requests.RequestException(
        f"Connection failed: https://example.invalid/?api_key={CANARY}"
    )
    with pytest.raises(RuntimeError, match="^FRED API request failed$") as error:
        oil_handler.module.publish_fred_data()
    rendered = "".join(traceback.format_exception(type(error.value), error.value, error.value.__traceback__))
    output = capsys.readouterr()
    assert CANARY not in rendered + output.out + output.err
    assert error.value.__suppress_context__ is True
    oil_handler.publisher.publish.assert_not_called()


def test_json_error_message_is_not_logged(oil_handler, capsys):
    response = SimpleNamespace(status_code=200, json=MagicMock(side_effect=ValueError(CANARY)))
    oil_handler.requests.get.return_value = response
    oil_handler.module.publish_fred_data()
    output = capsys.readouterr()
    assert CANARY not in output.out + output.err
    assert "ValueError" in output.out
    oil_handler.publisher.publish.assert_not_called()


def test_publish_error_message_is_not_logged(oil_handler, capsys):
    oil_handler.requests.get.return_value = SimpleNamespace(
        status_code=200, json=lambda: {"observations": [{"date": "2026-10-20", "value": "72.50"}]}
    )
    oil_handler.publisher.publish.return_value.result.side_effect = RuntimeError(CANARY)
    oil_handler.module.publish_fred_data()
    output = capsys.readouterr()
    assert CANARY not in output.out + output.err
    assert "Failed to publish Avro message (RuntimeError)" in output.out


def test_environment_fallback_does_not_log_secret_lookup_error(oil_handler, monkeypatch, capsys):
    oil_handler.module.get_secret.side_effect = RuntimeError(CANARY)
    monkeypatch.setenv("FRED_OIL_API_KEY", CANARY)
    oil_handler.requests.get.return_value = SimpleNamespace(
        status_code=200, json=lambda: {"observations": []}
    )
    oil_handler.module.publish_fred_data()
    output = capsys.readouterr()
    assert CANARY not in output.out + output.err
    assert oil_handler.requests.get.call_args.kwargs["params"]["api_key"] == CANARY
    oil_handler.publisher.publish.assert_not_called()


def test_missing_key_does_not_make_http_or_publish_calls(oil_handler, capsys):
    oil_handler.module.get_secret.return_value = ""
    oil_handler.module.publish_fred_data()
    assert "No FRED API key available" in capsys.readouterr().out
    oil_handler.requests.get.assert_not_called()
    oil_handler.publisher.publish.assert_not_called()
