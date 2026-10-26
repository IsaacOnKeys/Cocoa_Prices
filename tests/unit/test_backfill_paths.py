"""Resolve submission dependencies without importing or running Beam pipelines."""

import ast
import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("script", [
    "clean_cocoa_prices.py", "clean_oil_fred.py", "clean_precipitation.py",
])
def test_requirements_reference_survives_relocation(script):
    source = ROOT / "pipelines/backfill" / script
    tree = ast.parse(source.read_bytes())
    reference = next(
        node.value for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "REQUIREMENTS_FILE"
                for target in node.targets)
    )
    resolved = Path(eval(
        compile(ast.Expression(reference), str(source), "eval"),
        {"os": os, "__file__": str(source)},
    ))
    assert resolved == ROOT / "pipelines/requirements.txt"
    assert resolved.is_file()
