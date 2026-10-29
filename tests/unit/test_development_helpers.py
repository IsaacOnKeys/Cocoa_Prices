"""Verify relocated helpers against stub consumers, without cloud calls or kills."""

import os
import shutil
import subprocess
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
    started = subprocess.run(
        [bash, str(helpers / "stream_start")], cwd=tmp_path,
        capture_output=True, text=True,
    )
    assert started.returncode == 1 and 'Daily Beam streaming is retired' in started.stderr
    pid_file = fixture_root / "stream_pids.txt"
    assert not pid_file.exists()
    assert not list(fixture_root.glob('*.log'))
    # The stop helper can still clean a pre-existing legacy PID file.
    pids = ['23121', '23122', '23123']
    pid_file.write_text('\n'.join(pids)+'\n')
    stopped = subprocess.run(
        [bash, "-c", 'kill() { printf "stub-kill:%s\\n" "$1"; }; source "$1"',
         "_", (helpers / "stream_stop").as_posix()],
        cwd=tmp_path, check=True, capture_output=True, text=True,
    )
    assert stopped.stdout.splitlines() == [f"stub-kill:{pid}" for pid in pids]
    assert not pid_file.exists()
    assert not (tmp_path / "stream_pids.txt").exists()
