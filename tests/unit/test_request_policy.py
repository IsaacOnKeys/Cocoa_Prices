"""Exercise retry/deadline behavior with a fake clock and credential canaries."""

import importlib.util
import traceback
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

SPEC = importlib.util.spec_from_file_location(
    "policy_under_test", Path(__file__).resolve().parents[2] / "functions/shared/request_policy.py"
)
POLICY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(POLICY)
CANARY = "private-query-key-and-response-canary"


@pytest.fixture
def clock(monkeypatch):
    now = [0.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(POLICY.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(POLICY.time, "sleep", sleep)
    return SimpleNamespace(now=now, sleeps=sleeps)


def response(status, **headers):
    return SimpleNamespace(status_code=status, headers=headers, text=CANARY)


def client(*replies):
    return SimpleNamespace(RequestException=TimeoutError, get=Mock(side_effect=replies))


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
def test_transient_status_retries_then_returns_success(clock, status):
    ok = response(200)
    requests = client(response(status), response(status), ok)
    assert POLICY.get(requests, CANARY, deadline=40, label="Source") is ok
    assert clock.sleeps == [0.5, 1.0]
    assert [c.kwargs["timeout"] for c in requests.get.call_args_list] == [(5, 20), (5, 19.75), (5, 19.25)]


@pytest.mark.parametrize("reply", [response(429), response(503), TimeoutError(CANARY)])
def test_persistent_failure_has_exactly_three_attempts_and_safe_traceback(clock, reply, capsys):
    requests = client(reply, reply, reply)
    with pytest.raises(RuntimeError) as error:
        POLICY.get(requests, CANARY, deadline=40, label="Source")
    assert requests.get.call_count == 3
    assert clock.sleeps == [0.5, 1.0]
    rendered = "".join(traceback.format_exception(error.value))
    assert CANARY not in rendered + capsys.readouterr().out


def test_transport_timeout_can_recover(clock):
    requests = client(TimeoutError(CANARY), response(200))
    assert POLICY.get(requests, CANARY, deadline=40, label="Source").status_code == 200
    assert requests.get.call_count == 2


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_permanent_status_does_not_retry(clock, status):
    requests = client(response(status))
    with pytest.raises(RuntimeError, match=f"HTTP status {status}"):
        POLICY.get(requests, CANARY, deadline=40, label="Source")
    assert requests.get.call_count == 1 and clock.sleeps == []


def test_retry_after_is_honored(clock):
    requests = client(response(429, **{"Retry-After": "6"}), response(200))
    POLICY.get(requests, CANARY, deadline=40, label="Source")
    assert clock.sleeps == [6]


def test_http_date_retry_after_is_honored(clock, monkeypatch):
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 28, 17, tzinfo=timezone.utc)

    monkeypatch.setattr(POLICY, "datetime", Clock)
    requests = client(response(429, **{"Retry-After": "Wed, 28 Oct 2026 17:00:04 GMT"}), response(200))
    POLICY.get(requests, CANARY, deadline=40, label="Source")
    assert clock.sleeps == [4]


def test_invalid_retry_after_uses_backoff(clock):
    requests = client(response(503, **{"Retry-After": CANARY}), response(200))
    POLICY.get(requests, CANARY, deadline=40, label="Source")
    assert clock.sleeps == [0.5]


def test_retry_after_beyond_budget_fails_without_sleep_or_early_retry(clock):
    requests = client(response(429, **{"Retry-After": "60"}))
    with pytest.raises(RuntimeError, match="HTTP status 429"):
        POLICY.get(requests, CANARY, deadline=40, label="Source")
    assert requests.get.call_count == 1 and clock.sleeps == []


def test_deadline_is_shared_by_attempts(clock):
    def slow_request(*args, **kwargs):
        clock.now[0] += 3
        raise TimeoutError(CANARY)

    requests = client()
    requests.get.side_effect = slow_request
    with pytest.raises(RuntimeError, match="deadline exceeded"):
        POLICY.get(requests, CANARY, deadline=5, label="Source")
    assert requests.get.call_count == 2
    assert requests.get.call_args.kwargs["timeout"] == (0.75, 0.75)


def test_no_request_or_publish_wait_after_deadline(clock):
    requests = client()
    with pytest.raises(RuntimeError, match="deadline exceeded"):
        POLICY.get(requests, CANARY, deadline=0, label="Source")
    requests.get.assert_not_called()
    with pytest.raises(RuntimeError, match="deadline exceeded"):
        POLICY.publish_timeout(0)
    assert POLICY.publish_timeout(6) == 6
    assert POLICY.publish_timeout(50) == 10


def test_response_after_deadline_is_rejected(clock):
    def late_response(*args, **kwargs):
        clock.now[0] = 41
        return response(200)

    requests = client()
    requests.get.side_effect = late_response
    with pytest.raises(RuntimeError, match="deadline exceeded"):
        POLICY.get(requests, CANARY, deadline=40, label="Source")
