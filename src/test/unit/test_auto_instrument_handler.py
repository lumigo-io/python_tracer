import imp

import traceback

import mock

import pytest
from lumigo_tracer.auto_instrument_handler import _handler, ORIGINAL_HANDLER_KEY


def abc(*args, **kwargs):
    return {"hello": "world"}


def test_happy_flow(monkeypatch):
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "test_auto_instrument_handler.abc")
    assert _handler({}, {}) == {"hello": "world"}


def test_hierarchy_happy_flow(monkeypatch):
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "lumigo_tracer/test_module/test.handler")
    assert _handler({}, {}) == {"hello": "world"}


def test_import_error(monkeypatch):
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "blabla.not.exists")

    try:
        _handler({}, {})
    except ImportError as e:
        # Note: We're not using pytest.raises in order to get the exception context
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
    monkeypatch.setattr(imp, "load_module", mock.Mock(side_effect=ZeroDivisionError))
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "sys.exit")

    try:
        _handler({}, context)
    except ZeroDivisionError:
        # Note: We're not using pytest.raises in order to get the exception context
        assert "another exception occurred" not in traceback.format_exc()
    else:
        assert False


def test_error_in_original_handler_syntax_error(monkeypatch, context):
    monkeypatch.setattr(imp, "load_module", mock.Mock(side_effect=SyntaxError))
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "sys.exit")

    try:
        _handler({}, context)
    except SyntaxError as e:
        # Note: We're not using pytest.raises in order to get the exception context
        assert "Runtime.UserCodeSyntaxError" in str(e)
        assert "another exception occurred" not in traceback.format_exc()
    else:
        assert False


def test_handler_bad_format(monkeypatch):
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "no_method")

    try:
        _handler({}, {})
    except ValueError as e:
        # Note: We're not using pytest.raises in order to get the exception context
        assert "Runtime.MalformedHandlerName" in str(e)
        assert "another exception occurred" not in traceback.format_exc()
    else:
        assert False


def test_handler_not_found(monkeypatch):
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "sys.not_found")

    try:
        _handler({}, {})
    except Exception as e:
        # Note: We're not using pytest.raises in order to get the exception context
        assert "Runtime.HandlerNotFound" in str(e)
        assert "another exception occurred" not in traceback.format_exc()
    else:
        assert False
