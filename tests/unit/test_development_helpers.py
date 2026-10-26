"""Verify relocated helpers against stub consumers, without cloud calls or kills."""

import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_helpers_resolve_repository_and_share_pid_location(tmp_path):
    git_bash = Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Git/bin/bash.exe"
    bash = str(git_bash) if os.name == "nt" and git_bash.exists() else shutil.which("bash")
    if bash is None:
        pytest.skip("A Bash installation is required for helper path validation")
    fixture_root = tmp_path / "repository with spaces"
    helpers = fixture_root / "scripts/development"
    helpers.mkdir(parents=True)
    for name in ("stream_start", "stream_stop"):
        shutil.copyfile(ROOT / "scripts/development" / name, helpers / name)
    feeds = ("cocoa", "oil", "weather")
    (fixture_root / "pipelines/streaming").mkdir(parents=True)
    for feed in feeds:
        (fixture_root / f"pipelines/streaming/{feed}_stream.py").write_text("# stub; never imported\n")
    posix_root = subprocess.run(
        [bash, "-c", 'cd "$1" && pwd', "_", fixture_root.as_posix()],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    subprocess.run(
        [bash, "-c", 'python() { printf "%s\\n" "$1"; }; source "$1"',
         "_", (helpers / "stream_start").as_posix()],
        cwd=tmp_path, check=True, capture_output=True, text=True,
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if all((fixture_root / f"{feed}.log").exists()
               and (fixture_root / f"{feed}.log").stat().st_size for feed in feeds):
            break
        time.sleep(0.01)
    for feed in feeds:
        assert (fixture_root / f"{feed}.log").read_text().strip() == (
            f"{posix_root}/pipelines/streaming/{feed}_stream.py"
        )
    pid_file = fixture_root / "stream_pids.txt"
    pids = pid_file.read_text().splitlines()
    assert len(pids) == 3 and all(pid.isdigit() for pid in pids)
    stopped = subprocess.run(
        [bash, "-c", 'kill() { printf "stub-kill:%s\\n" "$1"; }; source "$1"',
         "_", (helpers / "stream_stop").as_posix()],
        cwd=tmp_path, check=True, capture_output=True, text=True,
    )
    assert stopped.stdout.splitlines() == [f"stub-kill:{pid}" for pid in pids]
    assert not pid_file.exists()
    assert not (tmp_path / "stream_pids.txt").exists()
