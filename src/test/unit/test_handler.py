import importlib
import traceback

import mock
import sys
import os

import pytest
from lumigo_tracer.sync_http.handler import _handler, ORIGINAL_HANDLER_KEY


def test_happy_flow(monkeypatch):
    m = mock.Mock(return_value={"hello": "world"})
    monkeypatch.setattr(sys, "exit", m)
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "sys.exit")

    assert _handler({}, {}) == {"hello": "world"}

    m.assert_called_once()


def test_hierarchy_happy_flow(monkeypatch):
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "os/path.getsize")
    m = mock.Mock(return_value={"hello": "world"})
    monkeypatch.setattr(os.path, "getsize", m)

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


def test_error_in_original_handler_no_extra_exception_log(monkeypatch, context):
    monkeypatch.setattr(importlib, "import_module", mock.Mock(side_effect=Exception))
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "sys.exit")
    exception_occurred = False
    try:
        _handler({}, context)
    except Exception:
        exception_occurred = True
        assert "another exception occurred" not in traceback.format_exc()
    assert exception_occurred is True
