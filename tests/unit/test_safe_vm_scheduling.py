"""Only the new execution windows and runtime start/stop behavior."""

import ast
import runpy
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def window():
    class Fail(Exception):
        pass

    class Clock(datetime):
        current = datetime(2026, 10, 30, 18, 10, tzinfo=timezone.utc)

        @classmethod
        def now(cls, tz=None):
            return cls.current.astimezone(tz)

    scope = {"datetime": Clock, "timezone": timezone, "TZ": ZoneInfo("Europe/Berlin"),
             "AirflowFailException": Fail,
             "_run_scope": Mock(return_value={"business_date": "2026-10-30", "engineering_validation": False}),
             "capture_baseline_fn": Mock(), "_invoke_producer": Mock(), "run_release_fn": Mock()}
    tree = ast.parse((ROOT / "airflow/dags/cocoa_daily.py").read_bytes())
    names = {"_check_service_window", "capture_with_window", "trigger_with_window", "release_with_window"}
    exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names],
                            type_ignores=[]), "service_window", "exec"), scope)
    return SimpleNamespace(ns=scope, clock=Clock, fail=Fail)


def test_delayed_start_accepted_before_cutoff_then_blocked(window):
    w = window
    w.ns["capture_with_window"]("ti", "run")  # 19:10 Berlin
    w.ns["capture_baseline_fn"].assert_called_once_with("ti", "run")
    w.clock.current = datetime(2026, 10, 30, 18, 30, tzinfo=timezone.utc)
    with pytest.raises(w.fail, match="window closed"):
        w.ns["capture_with_window"]("ti", "run")
    assert w.ns["capture_baseline_fn"].call_count == 1


def test_previous_day_restart_cannot_trigger_producers(window):
    w = window
    w.clock.current = datetime(2026, 10, 31, 18, tzinfo=timezone.utc)
    with pytest.raises(w.fail):
        w.ns["trigger_with_window"]("oil", trigger_topic="topic", dag_run="run")
    w.ns["_invoke_producer"].assert_not_called()


def test_release_cutoff_prevents_new_warehouse_work(window):
    w = window
    w.clock.current = datetime(2026, 10, 30, 19, 4, tzinfo=timezone.utc)
    w.ns["release_with_window"]("ti", "run")
    w.clock.current = datetime(2026, 10, 30, 19, 5, tzinfo=timezone.utc)
    with pytest.raises(w.fail):
        w.ns["release_with_window"]("ti", "run")
    assert w.ns["run_release_fn"].call_count == 1


def test_berlin_summer_time_and_engineering_opt_in(window):
    w = window
    w.ns["_run_scope"].return_value["business_date"] = "2026-07-01"
    w.clock.current = datetime(2026, 7, 1, 17, 29, tzinfo=timezone.utc)
    w.ns["capture_with_window"]("ti", "run")
    w.clock.current = datetime(2026, 7, 1, 17, 30, tzinfo=timezone.utc)
    with pytest.raises(w.fail):
        w.ns["capture_with_window"]("ti", "run")
    w.ns["_run_scope"].return_value["engineering_validation"] = True
    w.ns["capture_with_window"]("ti", "run")


def test_internal_warehouse_retry_rechecks_window_before_submission(monkeypatch):
    from test_atomic_airflow_delivery import adapter as adapter_fixture

    a = adapter_fixture.__wrapped__(monkeypatch)
    a.client.get_job.side_effect = [a.job(state="DONE", error={"reason": "backendError"}), a.NotFound()]
    guard = Mock(side_effect=a.permanent("Service execution window closed"))
    with pytest.raises(a.permanent, match="window closed"):
        a.scope["_run_release_job"](a.client, a.request, before_submit=guard)
    guard.assert_called_once_with()
    a.client.query.assert_not_called()


@pytest.fixture
def runtime(monkeypatch):
    ns = runpy.run_path(str(ROOT / "scripts/operations/airflow_runtime.py"))
    ns = ns["start"].__globals__
    command, emit = Mock(), Mock()
    monkeypatch.setitem(ns, "command", command)
    monkeypatch.setitem(ns, "emit", emit)
    return SimpleNamespace(ns=ns, command=command, emit=emit)


def test_missing_credential_prevents_compose_start(runtime):
    r = runtime
    r.command.return_value = ""
    with pytest.raises(RuntimeError, match="Fernet"):
        r.ns["start"]()
    assert r.command.call_count == 1
    r.emit.assert_not_called()


def test_delayed_health_failure_is_bounded_and_not_ready(runtime):
    r = runtime
    r.command.side_effect = ["test-key", TimeoutError("dependency unhealthy")]
    with pytest.raises(TimeoutError):
        r.ns["start"]()
    assert r.command.call_args.args[1] == 250
    r.emit.assert_not_called()


def test_start_checks_health_and_credentials_without_printing_key(runtime):
    r = runtime
    r.command.side_effect = ["private-test-key", "", "provider logs\nREADY"]
    r.ns["start"]()
    assert r.command.call_args.args[2]["AIRFLOW__CORE__FERNET_KEY"] == "private-test-key"
    r.emit.assert_called_once()
    assert "private-test-key" not in str(r.emit.call_args)


def test_active_run_shutdown_records_recovery_then_stops_without_secrets(runtime):
    r = runtime
    r.command.side_effect = ['[{"run_id":"probe","state":"running"}]', ""]
    r.ns["stop"]()
    assert r.emit.call_args.args == ("shutdown_interrupted", "ERROR")
    assert r.emit.call_args.kwargs["runs"][0]["run_id"] == "probe"
    calls = r.command.call_args_list
    assert "stop" in calls[-1].args[0]
    assert not any("gcloud" in call.args[0] for call in calls)


def test_unavailable_metadata_does_not_prevent_cost_cutoff(runtime):
    r = runtime
    r.command.side_effect = [TimeoutError(), ""]
    r.ns["stop"]()
    assert r.emit.call_args.args == ("shutdown_state_unknown", "ERROR")
    assert "stop" in r.command.call_args.args[0]


def test_idle_shutdown_is_quiet(runtime):
    r = runtime
    r.command.side_effect = ["[]", ""]
    r.ns["stop"]()
    r.emit.assert_not_called()
