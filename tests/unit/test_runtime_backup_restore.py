"""Recovery rejects unsafe/corrupt evidence before touching runtime state."""

import importlib
import io
import json
from pathlib import Path
import tarfile
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.fixture
def tools(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "scripts/operations"))
    return SimpleNamespace(backup=importlib.import_module("airflow_backup"), restore=importlib.import_module("airflow_restore"))


def bundle(backup, name="postgres.dump", content=b"database", corrupt=False, duplicate=False, symlink=False):
    body = io.BytesIO()
    manifest = {"version": 1, "files": {name: backup.sha(b"different" if corrupt else content)}}
    with tarfile.open(fileobj=body, mode="w:gz") as tar:
        member = tarfile.TarInfo(name)
        member.size = len(content)
        if symlink:
            member.type, member.linkname, member.size = tarfile.SYMTYPE, "/etc/passwd", 0
        tar.addfile(member, None if symlink else io.BytesIO(content))
        if duplicate:
            tar.addfile(member, io.BytesIO(content))
        data = json.dumps(manifest).encode()
        member = tarfile.TarInfo("manifest.json")
        member.size = len(data)
        tar.addfile(member, io.BytesIO(data))
    return body.getvalue()


def test_verified_archive_materializes_only_matching_bytes(tools, tmp_path):
    result = tools.backup.extract_verified(bundle(tools.backup), tmp_path)
    assert result["version"] == 1 and (tmp_path / "postgres.dump").read_bytes() == b"database"


@pytest.mark.parametrize("name", ["../escape", "/absolute", "C:/escape", "runtime\\escape"])
def test_unsafe_paths_are_rejected_without_writes(tools, tmp_path, name):
    with pytest.raises(RuntimeError, match="Unsafe"):
        tools.backup.extract_verified(bundle(tools.backup, name=name), tmp_path)
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("options", [{"corrupt": True}, {"duplicate": True}, {"symlink": True}])
def test_corruption_duplicates_and_links_are_rejected_before_writes(tools, tmp_path, options):
    with pytest.raises(RuntimeError):
        tools.backup.extract_verified(bundle(tools.backup, **options), tmp_path)
    assert not list(tmp_path.iterdir())


def test_restore_never_overwrites_an_existing_destination(tools, tmp_path):
    retained = tmp_path / "keep"
    retained.write_bytes(b"accepted")
    with pytest.raises(RuntimeError, match="destination"):
        tools.backup.extract_verified(bundle(tools.backup), tmp_path)
    assert retained.read_bytes() == b"accepted"


def test_mutable_or_foreign_key_reference_cannot_request_secret(tools, monkeypatch, tmp_path):
    receipt = {"version": 1, "bucket": tools.backup.BUCKET,
               "secret_version": f"projects/{tools.backup.PROJECT}/secrets/airflow-fernet-key/versions/latest"}
    calls = []

    def command(args, *_, **__):
        calls.append(args)
        return json.dumps(receipt).encode()

    monkeypatch.setattr(tools.restore, "command", command)
    with pytest.raises(RuntimeError, match="immutable key"):
        tools.restore.read_backup({"object": "runtime/test.json", "generation": 1}, tmp_path)
    assert len(calls) == 1 and "secrets" not in calls[0]


@pytest.mark.parametrize("numeric_project", [False, True])
def test_changed_ciphertext_is_rejected_before_key_access_or_docker(tools, monkeypatch, tmp_path, numeric_project):
    project = tools.backup.PROJECT_NUMBER if numeric_project else tools.backup.PROJECT
    receipt = {"version": 1, "bucket": tools.backup.BUCKET,
               "secret_version": f"projects/{project}/secrets/airflow-fernet-key/versions/1",
               "state": {"object": "runtime/test.fernet", "generation": "1", "size": 3, "sha256": "0" * 64}}
    calls = []

    def command(args, *_, **__):
        calls.append(args)
        if "cat" in args:
            return json.dumps(receipt).encode()
        Path(args[-1]).write_bytes(b"bad")
        return b""

    monkeypatch.setattr(tools.restore, "command", command)
    with pytest.raises(RuntimeError, match="Ciphertext"):
        tools.restore.read_backup({"object": "runtime/test.json", "generation": 1}, tmp_path)
    assert len(calls) == 2 and not any(args[0] == "docker" or "secrets" in args for args in calls)


def test_recovery_key_version_is_passed_without_key_material(tools, monkeypatch):
    command = Mock(return_value=b"result")
    monkeypatch.setenv("COCOA_FERNET_VERSION", "7")
    monkeypatch.setattr(tools.backup, "command", command)
    tools.backup.worker_call("seal", b"archive")
    assert "COCOA_FERNET_VERSION=7" in command.call_args.args[0]


def test_runtime_start_can_use_restored_immutable_key_reference(monkeypatch):
    import runpy

    source = Path(__file__).resolve().parents[2] / "scripts/operations/airflow_runtime.py"
    ns = runpy.run_path(str(source))["start"].__globals__
    command = Mock(side_effect=["test-key", "", "READY"])
    monkeypatch.setenv("COCOA_FERNET_VERSION", "7")
    monkeypatch.setitem(ns, "command", command)
    monkeypatch.setitem(ns, "emit", Mock())
    ns["start"]()
    assert command.call_args_list[0].args[0][4] == "7"


def test_download_staging_is_separate_from_verified_extraction(tools, monkeypatch):
    extracted = []

    def read_backup(_reference, temporary):
        (temporary / "state.fernet").write_bytes(b"ciphertext")
        return {}, bundle(tools.backup), "test-key"

    original = tools.backup.extract_verified

    def extract(data, destination):
        assert not destination.exists()
        manifest = original(data, destination)
        extracted.append(destination)
        return {**manifest, "images": {}}

    def command(*_, **__):
        raise RuntimeError("stop before creating Docker resources")

    monkeypatch.setattr(tools.restore, "read_backup", read_backup)
    monkeypatch.setattr(tools.restore, "extract_verified", extract)
    monkeypatch.setattr(tools.restore, "command", command)
    with pytest.raises(RuntimeError, match="before creating Docker"):
        tools.restore.restore_check({"object": "runtime/test.json", "generation": 1})
    assert len(extracted) == 1
