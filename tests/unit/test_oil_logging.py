"""Exercise the packaged EIA producer, including secret-safe failure paths."""

import importlib.util
import io
import json
import sys
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import fastavro
import pytest

ROOT = Path(__file__).resolve().parents[2]
CANARY = "test-only-eia-key-must-never-appear-in-output"


def row(period="2026-10-20", value="115.22", **changes):
    return {"period": period, "value": value, "series": "RBRTE", "units": "$/BBL", **changes}


def page(rows, total=None):
    return SimpleNamespace(
        status_code=200,
        json=lambda: {"response": {"data": rows, "total": str(len(rows) if total is None else total)}},
    )


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
    requests.get = MagicMock(return_value=page([row()]))
    for module in (google, cloud, cloud.pubsub_v1, cloud.secretmanager, requests):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setenv("GCP_PROJECT", "unit-test-project")
    monkeypatch.delenv("EIA_OIL_API_KEY", raising=False)
    monkeypatch.delenv("OIL_TOPIC", raising=False)
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
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 27, 18, tzinfo=timezone.utc).astimezone(tz)

    module.datetime = FixedDateTime
    module.get_secret = MagicMock(return_value=CANARY)
    verify = module.verify_against_published
    module.verify_against_published = MagicMock()
    return SimpleNamespace(module=module, requests=requests, publisher=publisher, verify=verify)


def decoded_publications(handler):
    schema = fastavro.parse_schema(json.loads((ROOT / "schemas/oil_schema.avsc").read_bytes()))
    return [fastavro.schemaless_reader(io.BytesIO(call.args[1]), schema)
            for call in handler.publisher.publish.call_args_list]


def assert_secret_safe(error, capsys):
    rendered = "".join(traceback.format_exception(type(error.value), error.value, error.value.__traceback__))
    output = capsys.readouterr()
    assert CANARY not in rendered + output.out + output.err


def test_success_publishes_entire_window_and_keeps_key_out_of_logs(oil_handler, capsys):
    oil_handler.requests.get.return_value = page([row(), row("2026-10-19", "118.82")])
    result = oil_handler.module.publish_eia_data()
    assert result == {"source": "EIA", "status": "published", "published": 2}
    assert CANARY not in capsys.readouterr().out
    oil_handler.module.get_secret.assert_called_once_with("EIA_OIL_API_KEY")
    args, kwargs = oil_handler.requests.get.call_args
    assert args == ("https://api.eia.gov/v2/petroleum/pri/spt/data/",)
    assert kwargs["params"]["api_key"] == CANARY
    assert kwargs["params"]["facets[series][]"] == "RBRTE"
    assert kwargs["params"]["frequency"] == "daily"
    assert kwargs["params"]["sort[0][direction]"] == "asc"
    assert kwargs["timeout"] == (5, 20)
    start, end = (date.fromisoformat(kwargs["params"][key]) for key in ("start", "end"))
    assert (end - start).days == 14
    records = decoded_publications(oil_handler)
    assert [(r["date"], r["oil_price"]) for r in records] == [
        ("2026-10-19", 118.82), ("2026-10-20", 115.22),
    ]
    assert all(r["ingestion_time"].endswith("+00:00") for r in records)
    assert all(json.loads(r["raw_payload"])["source"] == "EIA" for r in records)
    for call in oil_handler.publisher.publish.call_args_list:
        assert call.args[0] == "projects/unit-test-project/topics/oil-prices-topic"
        assert call.kwargs == {"googclient_schemaencoding": "BINARY"}
    oil_handler.publisher.publish.return_value.result.assert_called_with(timeout=10)


def test_pagination_uses_explicit_offsets_without_losing_rows(oil_handler):
    oil_handler.requests.get.side_effect = [
        page([row("2026-10-19")], 2), page([row()], 2),
    ]
    result = oil_handler.module.fetch_observations(CANARY, date(2026, 10, 1), date(2026, 10, 27), page_size=1)
    assert [r["date"] for r in result] == ["2026-10-19", "2026-10-20"]
    assert [c.kwargs["params"]["offset"] for c in oil_handler.requests.get.call_args_list] == [0, 1]


@pytest.mark.parametrize("response", [
    SimpleNamespace(status_code=403, text=f"api_key={CANARY}"),
    SimpleNamespace(status_code=429, text=CANARY),
    SimpleNamespace(status_code=503, text=CANARY),
])
def test_http_failures_propagate_without_logging_bodies(oil_handler, capsys, response):
    oil_handler.requests.get.return_value = response
    with pytest.raises(RuntimeError, match="EIA API HTTP status") as error:
        oil_handler.module.publish_eia_data()
    assert_secret_safe(error, capsys)
    oil_handler.publisher.publish.assert_not_called()


def test_request_failure_traceback_does_not_expose_key(oil_handler, capsys):
    oil_handler.requests.get.side_effect = oil_handler.requests.RequestException(
        f"Connection failed: https://example.invalid/?api_key={CANARY}"
    )
    with pytest.raises(RuntimeError, match="^EIA API request failed$") as error:
        oil_handler.module.publish_eia_data()
    assert_secret_safe(error, capsys)


@pytest.mark.parametrize("response", [
    SimpleNamespace(status_code=200, json=MagicMock(side_effect=ValueError(CANARY))),
    SimpleNamespace(status_code=200, json=lambda: {"error": CANARY}),
    SimpleNamespace(status_code=200, json=lambda: {"response": {"data": [], "total": "1"}}),
    SimpleNamespace(status_code=200, json=lambda: []),
])
def test_invalid_or_incomplete_response_fails_safely(oil_handler, capsys, response):
    oil_handler.requests.get.return_value = response
    with pytest.raises(RuntimeError, match="Invalid or incomplete EIA response") as error:
        oil_handler.module.publish_eia_data()
    assert_secret_safe(error, capsys)
    oil_handler.publisher.publish.assert_not_called()


@pytest.mark.parametrize("changes", [
    {"series": "RWTC"}, {"units": "EUR/barrel"}, {"period": "2026-02-30"},
    {"period": "2026-11-01"}, {"period": None}, {"value": "nan"},
    {"value": "inf"}, {"value": True}, {"value": CANARY},
])
def test_incompatible_observation_cannot_be_published(oil_handler, changes):
    oil_handler.requests.get.return_value = page([row(**changes)])
    with pytest.raises(RuntimeError, match="Invalid or incomplete EIA response"):
        oil_handler.module.fetch_observations(CANARY, date(2026, 10, 1), date(2026, 10, 27))
    oil_handler.publisher.publish.assert_not_called()


@pytest.mark.parametrize("value", [None, "", ".", "NA", "--", "-"])
def test_explicit_missing_measurements_remain_nullable(oil_handler, value):
    oil_handler.requests.get.return_value = page([row(value=value)])
    oil_handler.module.publish_eia_data()
    assert decoded_publications(oil_handler)[0]["oil_price"] is None


def test_duplicate_or_changing_pages_fail(oil_handler):
    for pages in ([page([row()], 2), page([row()], 2)],
                  [page([row("2026-10-19")], 2), page([row()], 3)]):
        oil_handler.requests.get.side_effect = pages
        with pytest.raises(RuntimeError, match="Invalid or incomplete EIA response"):
            oil_handler.module.fetch_observations(CANARY, date(2026, 10, 1), date(2026, 10, 27), page_size=1)


def test_publish_error_propagates_without_exposing_key(oil_handler, capsys):
    oil_handler.publisher.publish.return_value.result.side_effect = RuntimeError(CANARY)
    with pytest.raises(RuntimeError, match="EIA Avro publication failed") as error:
        oil_handler.module.publish_eia_data()
    assert_secret_safe(error, capsys)


def test_environment_fallback_keeps_lookup_error_private(oil_handler, monkeypatch, capsys):
    oil_handler.module.get_secret.side_effect = RuntimeError(CANARY)
    monkeypatch.setenv("EIA_OIL_API_KEY", CANARY)
    oil_handler.module.publish_eia_data()
    assert CANARY not in capsys.readouterr().out
    assert oil_handler.requests.get.call_args.kwargs["params"]["api_key"] == CANARY


def test_missing_key_fails_without_http_or_publish_calls(oil_handler):
    oil_handler.module.get_secret.return_value = ""
    with pytest.raises(RuntimeError, match="No EIA API key available"):
        oil_handler.module.publish_eia_data()
    oil_handler.requests.get.assert_not_called()
    oil_handler.publisher.publish.assert_not_called()


def test_empty_source_window_is_explicit(oil_handler):
    oil_handler.requests.get.return_value = page([])
    assert oil_handler.module.publish_eia_data()["status"] == "no_observations"
    oil_handler.publisher.publish.assert_not_called()


def test_eia_wire_passes_existing_beam_decoder_and_row_mapping(oil_handler, monkeypatch):
    from test_streaming_consumers import load_consumer

    oil_handler.module.publish_eia_data()
    binary = oil_handler.publisher.publish.call_args.args[1]
    consumer = load_consumer(monkeypatch, "oil").module
    decoded = list(consumer.DecodeAvro(ROOT / "schemas/oil_schema.avsc").process(binary))
    mapped = list(consumer.ToBQRow().process(decoded[0]))[0]
    assert (mapped["date"], mapped["oil_price"]) == ("2026-10-20", 115.22)
    assert json.loads(mapped["raw_payload"])["series"] == "RBRTE"


def test_expired_deadline_makes_no_request(oil_handler):
    with pytest.raises(RuntimeError, match="deadline exceeded"):
        oil_handler.module.fetch_observations(CANARY, date(2026, 10, 1), date(2026, 10, 27), deadline=0)
    oil_handler.requests.get.assert_not_called()


PUBLISHED_FIXTURE = """<html><title>Europe Brent Spot Price FOB (Dollars per Barrel)</title>
<table><tr><td>2016 Dec-26 to Dec-30</td><td></td><td></td><td>54.95</td><td>54.97</td><td>54.96</td></tr>
<tr><td>2018 Apr-16 to Apr-20</td><td>72.05</td><td>71.71</td><td>73.73</td><td>74.85</td><td>74.62</td></tr></table></html>"""


def test_published_parser_preserves_weekday_alignment_and_holiday_blanks(oil_handler):
    records = oil_handler.module.parse_published_observations(
        PUBLISHED_FIXTURE, date(2016, 12, 26), date(2016, 12, 30)
    )
    assert [(r["date"], r["oil_price"]) for r in records] == [
        ("2016-12-28", 54.95), ("2016-12-29", 54.97), ("2016-12-30", 54.96),
    ]


def test_known_api_date_value_disagreement_is_rejected(oil_handler):
    oil_handler.requests.get.return_value = SimpleNamespace(status_code=200, text=PUBLISHED_FIXTURE)
    observation = oil_handler.module.normalize_observation(
        row("2018-04-16", "73.18"), date(2018, 4, 16), date(2018, 4, 16)
    )
    with pytest.raises(RuntimeError, match="observations disagree"):
        oil_handler.verify([observation], date(2018, 4, 16), date(2018, 4, 16))
    oil_handler.publisher.publish.assert_not_called()


def test_matching_published_window_is_marked_verified(oil_handler):
    oil_handler.requests.get.return_value = SimpleNamespace(status_code=200, text=PUBLISHED_FIXTURE)
    observation = oil_handler.module.normalize_observation(
        row("2018-04-16", "72.05"), date(2018, 4, 16), date(2018, 4, 16)
    )
    oil_handler.verify([observation], date(2018, 4, 16), date(2018, 4, 16))
    assert observation["published_verified"] is True


def test_corroboration_failure_blocks_entire_batch(oil_handler):
    oil_handler.module.verify_against_published.side_effect = RuntimeError("EIA observations disagree")
    with pytest.raises(RuntimeError, match="observations disagree"):
        oil_handler.module.publish_eia_data()
    oil_handler.publisher.publish.assert_not_called()


def test_published_error_page_is_not_an_empty_valid_window(oil_handler):
    with pytest.raises(RuntimeError, match="Unexpected EIA published product"):
        oil_handler.module.parse_published_observations("<html>Service unavailable</html>", date(2026, 1, 1), date(2026, 1, 2))
