"""The partial-batch probe preserves PublisherClient's static topic-path API."""

import ast
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


def test_real_probe_publishes_first_row_before_injected_second_failure():
    source = Path(__file__).resolve().parents[2]/'scripts/diagnostics/coverage_probe.py'
    definition = next(n for n in ast.walk(ast.parse(source.read_bytes())) if isinstance(n, ast.ClassDef) and n.name == 'Publisher')
    real = SimpleNamespace(topic_path=lambda project, topic: project+'/'+topic, publish=Mock())
    probe_publish = {'attempts': 0}
    scope = {'real': real, 'SimpleNamespace': SimpleNamespace, 'probe_publish': probe_publish}
    exec(compile(ast.Module(body=[definition], type_ignores=[]), str(source), 'exec'), scope)
    publisher = scope['Publisher']()
    assert publisher.topic_path('project', 'isolated-topic') == 'project/isolated-topic'
    assert publisher.publish('topic', b'first') is real.publish.return_value
    with pytest.raises(RuntimeError, match='Controlled publication failure'):
        publisher.publish('topic', b'second').result(timeout=10)
    real.publish.assert_called_once_with('topic', b'first')
    assert probe_publish['attempts'] == 2
