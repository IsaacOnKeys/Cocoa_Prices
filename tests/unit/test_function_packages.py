"""Exercise packaging without importing or invoking cloud producers."""

import ast
import importlib.util
import os
from hashlib import sha256
from pathlib import Path
from zipfile import ZipFile

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "package_functions", ROOT / "scripts/deploy/package_functions.py"
)
PACKAGER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PACKAGER)


@pytest.mark.parametrize("feed", ["cocoa", "oil", "weather"])
def test_complete_runtime_and_deterministic_archive(feed, tmp_path):
    source_dir, entry_point = PACKAGER.FUNCTIONS[feed]
    fixture_root = tmp_path / "repo"
    expected = {
        "main.py": (ROOT / source_dir / "main.py").read_bytes(),
        "requirements.txt": (ROOT / source_dir / "requirements.txt").read_bytes(),
        f"{feed}_schema.avsc": (ROOT / "schemas" / f"{feed}_schema.avsc").read_bytes(),
    }
    (fixture_root / source_dir).mkdir(parents=True)
    (fixture_root / "schemas").mkdir()
    for name, payload in expected.items():
        parent = fixture_root / ("schemas" if name.endswith(".avsc") else source_dir)
        (parent / name).write_bytes(payload)
    # Build inputs may contain private/generated files; only allowlisted files may ship.
    (fixture_root / source_dir / ".env").write_text("PRIVATE_FIXTURE=do-not-package\n")
    (fixture_root / source_dir / "__pycache__").mkdir()
    (fixture_root / source_dir / "__pycache__/stale.pyc").write_bytes(b"cache")
    runtime_root, archive = PACKAGER.package_function(feed, repo_root=fixture_root)
    assert {path.name for path in runtime_root.iterdir()} == set(expected)
    with ZipFile(archive) as bundle:
        assert bundle.namelist() == sorted(expected)
        for name, payload in expected.items():
            assert bundle.read(name) == payload == (runtime_root / name).read_bytes()
            assert sha256(bundle.read(name)).digest() == sha256(payload).digest()
    definitions = ast.parse((runtime_root / "main.py").read_bytes()).body
    assert entry_point in {node.name for node in definitions if isinstance(node, ast.FunctionDef)}
    first_archive = archive.read_bytes()
    for path in (fixture_root / source_dir).glob("*.py"):
        os.utime(path, (1700000000, 1700000000))
    PACKAGER.package_function(feed, repo_root=fixture_root)
    assert archive.read_bytes() == first_archive


def test_prepared_root_with_unexpected_files_is_rejected(tmp_path):
    runtime_root, _ = PACKAGER.package_function("cocoa", output_dir=tmp_path)
    private_fixture = runtime_root / ".env"
    private_fixture.write_text("PRIVATE_FIXTURE=do-not-package\n")
    with pytest.raises(ValueError, match="unexpected files"):
        PACKAGER.package_function("cocoa", output_dir=tmp_path)
    assert private_fixture.exists()
