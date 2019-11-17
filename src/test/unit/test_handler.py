import mock
import sys
import os

import pytest
from lumigo_tracer.sync_http.handler import handler, ORIGINAL_HANDLER_KEY


def test_happy_flow(monkeypatch):
    m = mock.Mock()
    monkeypatch.setattr(sys, "exit", m)
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "sys.exit")

    handler({}, {})

    m.assert_called_once()


def test_import_error(monkeypatch):
    monkeypatch.setenv(ORIGINAL_HANDLER_KEY, "blabla.not.exists")

    with pytest.raises(ImportError):
        handler({}, {})


def test_no_env_handler_error(monkeypatch):
    if os.environ.get(ORIGINAL_HANDLER_KEY):
        monkeypatch.delenv(ORIGINAL_HANDLER_KEY)

    with pytest.raises(ValueError):
        handler({}, {})
