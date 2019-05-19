import os
import urllib
from io import BytesIO
from types import SimpleNamespace

from capturer import CaptureOutput
from lumigo_tracer import lumigo_tracer
from lumigo_tracer.parsers.parser import Parser
import http.client
from lumigo_tracer import utils
import pytest

from lumigo_tracer.spans_container import SpansContainer


def test_lambda_wrapper_basic_events(reporter_mock):
    """
    This test checks that the basic events (start and end messages) has been sent.
    """

    @lumigo_tracer(token="123")
    def lambda_test_function():
        pass

    lambda_test_function()
    events = SpansContainer.get_span().events
    assert len(events) == 1
    assert "started" in events[0]
    assert "ended" in events[0]
    assert reporter_mock.call_count == 2
    first_send = reporter_mock.call_args_list[0][1]["msgs"]
    assert len(first_send) == 1
    assert first_send[0]["id"].endswith("_started")
    assert first_send[0]["maxFinishTime"]


@pytest.mark.parametrize("exc", [ValueError("Oh no"), ValueError()])
def test_lambda_wrapper_exception(exc):
    @lumigo_tracer(token="123")
    def lambda_test_function():
        raise exc

    try:
        lambda_test_function()
    except ValueError:
        pass
    else:
        assert False

    events = SpansContainer.get_span().events
    assert len(events) == 1
    assert events[0].get("error", {}).get("type") == "ValueError"
    assert not events[0]["id"].endswith("_started")
    assert "reporter_rtt" in events[0]
    assert "maxFinishTime" not in events[0]


def test_lambda_wrapper_http():
    @lumigo_tracer(token="123")
    def lambda_test_function():
        http.client.HTTPConnection("www.google.com").request("POST", "/")

    lambda_test_function()
    events = SpansContainer.get_span().events
    assert len(events) == 2
    assert events[1].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "started" in events[1]
    assert "ended" in events[1]
    assert "Content-Length" in events[1]["info"]["httpInfo"]["request"]["headers"]


def test_lambda_wrapper_no_string():
    @lumigo_tracer(token="123")
    def lambda_test_function():
        http.client.HTTPConnection("www.google.com").send(BytesIO(b"123"))

    lambda_test_function()
    events = SpansContainer.get_span().events
    assert len(events) == 2
    assert events[1].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "started" in events[1]
    assert "ended" in events[1]


def test_kill_switch(monkeypatch):
    monkeypatch.setattr(os, "environ", {"LUMIGO_SWITCH_OFF": "true"})

    @lumigo_tracer(token="123")
    def lambda_test_function():
        return 1

    assert lambda_test_function() == 1
    assert not SpansContainer._span


def test_wrapping_exception(monkeypatch):
    monkeypatch.setattr(SpansContainer, "create_span", lambda x: 1 / 0)

    @lumigo_tracer(token="123")
    def lambda_test_function():
        return 1

    assert lambda_test_function() == 1
    assert not SpansContainer._span


def test_wrapping_with_parameters():
    @lumigo_tracer(should_report="123")
    def lambda_test_function():
        return 1

    assert lambda_test_function() == 1
    assert utils._SHOULD_REPORT == "123"


def test_wrapping_with_print_override():
    @lumigo_tracer(enhance_print=True)
    def lambda_test_function(event, context):
        print("hello\nworld")
        return 1

    with CaptureOutput() as capturer:
        assert lambda_test_function({}, SimpleNamespace(aws_request_id="1234")) == 1
        assert utils._ENHANCE_PRINT is True
        assert any(
            line == "RequestId: 1234 hello\nRequestId: 1234 world" for line in capturer.get_lines()
        )


def test_wrapping_without_print_override():
    @lumigo_tracer()
    def lambda_test_function(event, context):
        print("hello")
        return 1

    with CaptureOutput() as capturer:
        assert lambda_test_function({}, SimpleNamespace(aws_request_id="1234")) == 1
        assert utils._ENHANCE_PRINT is False
        assert any(line == "hello" for line in capturer.get_lines())


def test_wrapping_json_request():
    @lumigo_tracer()
    def lambda_test_function():
        urllib.request.urlopen(
            urllib.request.Request(
                "http://api.github.com", b"{}", headers={"Content-Type": "application/json"}
            )
        )
        return 1

    assert lambda_test_function() == 1
    events = SpansContainer.get_span().events
    assert any(
        "'Content-Type': 'application/json'"
        in event.get("info", {}).get("httpInfo", {}).get("request", {}).get("headers", "")
        for event in events
    )


def test_exception_in_parsers(monkeypatch, caplog):
    monkeypatch.setattr(Parser, "parse_request", Exception)

    @lumigo_tracer(token="123")
    def lambda_test_function():
        return http.client.HTTPConnection(host="www.google.com").send(b"\r\n")

    lambda_test_function()
    assert caplog.records[-1].msg == "An exception occurred in lumigo's code add request event"
