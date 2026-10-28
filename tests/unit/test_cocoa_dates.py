"""Preserve real source observation dates through scraping, publication and the Avro contract."""

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
from bs4 import BeautifulSoup, FeatureNotFound

ROOT = Path(__file__).resolve().parents[2]


def html(rows):
    return '<table id="table_1"><tbody>' + ''.join(
        f'<tr id="table_{index}"><td>{index}</td><td>{date}</td><td>4,278.33</td>'
        f'<td>5,641.00</td><td>5,681.21</td><td>{price}</td></tr>'
        for index, (date, price) in enumerate(rows)
    ) + '</tbody></table>'


@pytest.fixture
def cocoa(monkeypatch, tmp_path):
    google = ModuleType("google")
    cloud = ModuleType("google.cloud")
    pubsub = ModuleType("google.cloud.pubsub_v1")
    google.cloud = cloud
    cloud.pubsub_v1 = pubsub
    publisher = Mock()
    publisher.topic_path.side_effect = lambda project, topic: f"projects/{project}/topics/{topic}"
    pubsub.PublisherClient = Mock(return_value=publisher)
    framework = ModuleType("functions_framework")
    framework.http = lambda fn: fn
    requests = ModuleType("requests")
    requests.get = Mock(return_value=SimpleNamespace(text=html([("23/10/2026", "5,075.02")])))
    for module in (google, cloud, pubsub, framework, requests):
        monkeypatch.setitem(sys.modules, module.__name__, module)
    runtime = tmp_path / "cocoa"
    runtime.mkdir()
    for name, source in (("main.py", "functions/cocoa/main.py"), ("cocoa_schema.avsc", "schemas/cocoa_schema.avsc")):
        (runtime / name).write_bytes((ROOT / source).read_bytes())
    spec = importlib.util.spec_from_file_location("cocoa_dates_under_test", runtime / "main.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 28, 8, tzinfo=timezone.utc)

    module.datetime = FixedDateTime
    return SimpleNamespace(module=module, requests=requests, publisher=publisher)


def test_previous_observation_date_is_preserved_in_wire_raw_and_response(cocoa):
    body, status, _ = cocoa.module.publish_price(None)
    assert status == 200 and json.loads(body) == {"date": "2026-10-23", "price": 5075.02}
    cocoa.publisher.publish.assert_called_once()
    binary = cocoa.publisher.publish.call_args.args[1]
    schema = json.loads((ROOT / "schemas/cocoa_schema.avsc").read_bytes())
    payload = fastavro.schemaless_reader(io.BytesIO(binary), schema)
    assert payload["date"] == "2026-10-23"
    assert payload["ingestion_time"] == "2026-10-28T08:00:00+00:00"
    assert json.loads(payload["raw_payload"]) == {"date": "2026-10-23", "price": 5075.02, "date_basis": "source_observation"}


def test_selects_latest_source_date_independent_of_row_order(cocoa):
    cocoa.requests.get.return_value.text = html([("23/10/2026", "5,075.02"), ("22/10/2026", "5,079.03")])
    observed, price = cocoa.module._scrape_price()
    assert observed.isoformat() == "2026-10-23" and price == 5075.02


def test_invalid_date_does_not_replace_valid_source_observation(cocoa):
    cocoa.requests.get.return_value.text = html([("23/10/2026", "5,075.02"), ("not-a-date", "6,000.00")])
    assert cocoa.module._scrape_price()[0].isoformat() == "2026-10-23"


@pytest.mark.parametrize("markup", ["<html>unavailable</html>", html([("invalid", "5,075.02")]), html([("23/10/2026", "NA")]), html([("23/10/2026", "NaN")])])
def test_missing_or_incompatible_observation_fails_without_publication(cocoa, markup):
    cocoa.requests.get.return_value.text = markup
    assert cocoa.module.publish_price(None)[1] == 500
    cocoa.publisher.publish.assert_not_called()


def test_html_parser_fallback_preserves_the_same_date(cocoa, monkeypatch):
    def parser(markup, name):
        if name == "lxml":
            raise FeatureNotFound("lxml not installed")
        return BeautifulSoup(markup, name)
    monkeypatch.setattr(cocoa.module, "BeautifulSoup", parser)
    observed, price = cocoa.module._scrape_price()
    assert observed.isoformat() == "2026-10-23" and price == 5075.02


def test_acquisition_failure_does_not_invent_today_as_an_observation(cocoa):
    cocoa.requests.get.side_effect = TimeoutError("source timeout")
    assert cocoa.module.publish_price(None)[1] == 500
    cocoa.publisher.publish.assert_not_called()
