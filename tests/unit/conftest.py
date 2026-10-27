"""Unit tests must never connect to cloud services or other network endpoints."""

import socket

import pytest


@pytest.fixture(autouse=True)
def block_network(monkeypatch):
    def deny_connection(*args, **kwargs):
        raise AssertionError("Network access is forbidden in the unit test suite")

    monkeypatch.setattr(socket, "create_connection", deny_connection)
    monkeypatch.setattr(socket.socket, "connect", deny_connection)
    monkeypatch.setattr(socket.socket, "connect_ex", deny_connection)
