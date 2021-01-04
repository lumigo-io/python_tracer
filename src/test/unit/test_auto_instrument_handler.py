import importlib
import traceback

import mock
import sys
import os

import pytest
from lumigo_tracer.auto_instrument_handler import _handler, ORIGINAL_HANDLER_KEY


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

    try:
        _handler({}, {})
    except ImportError as e:
        assert "Runtime.ImportModuleError" in str(e)
        assert "another exception occurred" not in traceback.format_exc()
    else:
        assert False


def test_no_env_handler_error(monkeypatch):
    monkeypatch.delenv(ORIGINAL_HANDLER_KEY, None)

    with pytest.raises(Exception) as e:
        _handler({}, {})
    assert "Could not find the original handler" in str(e.value)


def test_error_in_original_handler_no_extra_exception_log(monkeypatch, context):
    monkeypatch.setattr(importlib, "import_module", mock.Mock(side_effect=ZeroDivisionError))
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "sys.exit")

    try:
        _handler({}, context)
    except ZeroDivisionError:
        assert "another exception occurred" not in traceback.format_exc()
    else:
        assert False


def test_error_in_original_handler_syntax_error(monkeypatch, context):
    monkeypatch.setattr(importlib, "import_module", mock.Mock(side_effect=SyntaxError))
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "sys.exit")

    try:
        _handler({}, context)
    except SyntaxError as e:
        assert "Runtime.UserCodeSyntaxError" in str(e)
        assert "another exception occurred" not in traceback.format_exc()
    else:
        assert False


def test_handler_bad_format(monkeypatch):
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "no_method")

    try:
        _handler({}, {})
    except ValueError as e:
        assert "Runtime.MalformedHandlerName" in str(e)
        assert "another exception occurred" not in traceback.format_exc()
    else:
        assert False


def test_handler_not_found(monkeypatch):
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "sys.not_found")

    try:
        _handler({}, {})
    except Exception as e:
        assert "Runtime.HandlerNotFound" in str(e)
        assert "another exception occurred" not in traceback.format_exc()
    else:
        assert False
