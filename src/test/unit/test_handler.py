import importlib

import mock
import sys
import os

import pytest
from lumigo_tracer.sync_http.handler import _handler, ORIGINAL_HANDLER_KEY


def test_happy_flow(monkeypatch):
    m = mock.Mock()
    m.return_value = {"hello": "world"}
    monkeypatch.setattr(sys, "exit", m)
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "sys.exit")

    assert _handler({}, {}) == {"hello": "world"}

    m.assert_called_once()


def test_import_error(monkeypatch):
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "blabla.not.exists")

    with pytest.raises(ImportError):
        _handler({}, {})


def test_no_env_handler_error(monkeypatch):
    if os.environ.get(ORIGINAL_HANDLER_KEY):
        monkeypatch.delenv(ORIGINAL_HANDLER_KEY)

    with pytest.raises(ValueError):
        _handler({}, {})


def test_syntax_error_in_original_handler(monkeypatch, context):
    monkeypatch.setattr(importlib, "import_module", mock.Mock(side_effect=SyntaxError))
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "sys.exit")

    with pytest.raises(SyntaxError) as err:
        _handler({}, context)
    assert err.value.msg == "Syntax error in the original handler."
