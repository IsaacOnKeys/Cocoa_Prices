"""Compile repository Python without importing it, then parse shell helpers."""

import argparse
import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOTS = ("functions", "pipelines", "airflow/dags", "scripts", "tests")
SKIP_DIRECTORIES = {"__pycache__", ".venv", "venv", ".build", ".pytest_cache"}
SHELL_FILES = (
    "scripts/development/stream_start",
    "scripts/development/stream_stop",
    "scripts/development/stream_status",
    "scripts/operations/setup_fernet_secret.sh",
    "scripts/operations/restart_with_secrets.sh",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-only", action="store_true", help="Check Python without invoking Bash")
    args = parser.parse_args()
    sources = []
    for relative_root in SOURCE_ROOTS:
        for directory, children, filenames in os.walk(ROOT / relative_root):
            children[:] = sorted(name for name in children if name not in SKIP_DIRECTORIES)
            sources.extend(Path(directory) / name for name in filenames if name.endswith(".py"))
    for path in sorted(sources):
        compile(path.read_bytes(), str(path.relative_to(ROOT)), "exec")
    print(f"Python syntax: {len(sources)} files passed (no application imports)")
    if args.python_only:
        return
    git_bash = Path(os.environ.get("ProgramFiles", "C:/Program Files")) / "Git/bin/bash.exe"
    bash = str(git_bash) if os.name == "nt" and git_bash.exists() else shutil.which("bash")
    if bash is None:
        parser.error("Bash is required for shell syntax checks")
    for name in SHELL_FILES:
        subprocess.run([bash, "-n", str(ROOT / name)], check=True)
    print(f"Shell syntax: {len(SHELL_FILES)} files passed (no scripts executed)")


if __name__ == "__main__":
    main()
