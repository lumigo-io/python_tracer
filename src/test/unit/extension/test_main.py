import http.client
import urllib.request
from types import SimpleNamespace

import pytest
from mock import Mock

from lumigo_tracer.extension.main import (
    STOP_EXTENSION_KEY,
    main,
    register,
    start_extension_loop,
)
from lumigo_tracer.lumigo_utils import KILL_SWITCH


def test_extension_loop_happy_flow(mock_linux_files, reporter_mock, monkeypatch, lambda_service):
    """
    This test checks the case of two requests and shutdown.
    """
    http_mock = Mock()
    http_mock.read.side_effect = [
        '{"eventType": "INVOKE", "requestId": "1"}',
        '{"eventType": "INVOKE", "requestId": "2"}',
        '{"eventType": "SHUTDOWN"}',
    ]
    monkeypatch.setattr(urllib.request, "urlopen", lambda *args: http_mock)

    start_extension_loop(lambda_service)

    assert reporter_mock.call_count == 2
    assert reporter_mock.call_args_list[0][1]["msgs"][0]["requestId"] == "1"
    assert reporter_mock.call_args_list[1][1]["msgs"][0]["requestId"] == "2"


def test_register_happy_flow(monkeypatch):
    mock = Mock()
    mock("127.0.0.1").getresponse.return_value = SimpleNamespace(
        headers={"Lambda-Extension-Identifier": "eid"}, read=lambda: ""
    )
    monkeypatch.setattr(http.client, "HTTPConnection", mock)
    result = register()

    assert result == "eid"


@pytest.mark.parametrize(
    "kill_switch, extension_switch, extension_activated",
    [
        ("true", "true", False),
        ("true", "false", False),
        ("false", "true", False),
        ("false", "false", True),
        ("", "", True),
    ],
)
def test_extension_stopper(
    mock_linux_files,
    monkeypatch,
    reporter_mock,
    kill_switch,
    extension_switch,
    extension_activated,
):
    http_mock = Mock()
    http_mock("127.0.0.1").getresponse.return_value = SimpleNamespace(
        headers={"Lambda-Extension-Identifier": "eid"}, read=lambda: ""
    )
    http_mock.read.side_effect = [
        '{"eventType": "INVOKE", "requestId": "1"}',
        '{"eventType": "INVOKE", "requestId": "2"}',
        '{"eventType": "SHUTDOWN"}',
    ]
    monkeypatch.setattr(urllib.request, "urlopen", lambda *args: http_mock)
    monkeypatch.setattr(http.client, "HTTPConnection", http_mock)

    monkeypatch.setenv(KILL_SWITCH, kill_switch)
    monkeypatch.setenv(STOP_EXTENSION_KEY, extension_switch)

    main()

    assert bool(reporter_mock.call_count) == extension_activated
