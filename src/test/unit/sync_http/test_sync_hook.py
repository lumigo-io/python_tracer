import os
import sys
import urllib
from io import BytesIO
from types import SimpleNamespace
import logging

from capturer import CaptureOutput
from lumigo_tracer import lumigo_tracer, LumigoChalice
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


def test_lambda_wrapper_query_with_http_params():
    @lumigo_tracer(token="123")
    def lambda_test_function():
        http.client.HTTPConnection("www.google.com").request("GET", "/?q=123")

    lambda_test_function()
    events = SpansContainer.get_span().events

    assert len(events) == 2
    print(events[1]["info"]["httpInfo"]["request"])
    assert events[1]["info"]["httpInfo"]["request"]["uri"] == "www.google.com/?q=123"


def test_lambda_wrapper_get_response():
    @lumigo_tracer(token="123")
    def lambda_test_function():
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("GET", "")
        conn.getresponse()

    lambda_test_function()
    events = SpansContainer.get_span().events

    assert len(events) == 2
    assert events[1]["info"]["httpInfo"]["response"]["statusCode"] == 200


def test_lambda_wrapper_http_splitted_send():
    """
    This is a test for the specific case of requests, where they split the http requests into headers and body.
    We didn't use directly the package requests in order to keep the dependencies small.
    """

    @lumigo_tracer(token="123")
    def lambda_test_function():
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("POST", "/", b"123")
        conn.send(BytesIO(b"456"))

    lambda_test_function()
    events = SpansContainer.get_span().events
    assert len(events) == 2
    assert events[1]["info"]["httpInfo"]["request"]["body"] == "b'123456'"
    assert "Content-Length" in events[1]["info"]["httpInfo"]["request"]["headers"]


def test_lambda_wrapper_no_headers():
    @lumigo_tracer(token="123")
    def lambda_test_function():
        http.client.HTTPConnection("www.google.com").send(BytesIO(b"123"))

    lambda_test_function()
    events = SpansContainer.get_span().events
    assert len(events) == 2
    assert events[1].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "started" in events[1]
    assert "ended" in events[1]


def test_lambda_wrapper_http_non_splitted_send():
    @lumigo_tracer(token="123")
    def lambda_test_function():
        http.client.HTTPConnection("www.google.com").request("POST", "/")
        http.client.HTTPConnection("www.github.com").send(BytesIO(b"123"))

    lambda_test_function()
    events = SpansContainer.get_span().events
    assert len(events) == 3  # one for the function span, and another two http spans


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
        assert "RequestId: 1234 hello" in capturer.get_lines()
        assert "RequestId: 1234 world" in capturer.get_lines()


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


def test_lumigo_chalice():
    class App:
        @property
        def a(self):
            return "a"

        def b(self):
            return "b"

        def __call__(self, *args, **kwargs):
            return "c"

    app = App()
    app = LumigoChalice(app)

    # should not use lumigo's wrapper
    assert app.a == "a"
    assert app.b() == "b"
    assert not SpansContainer.get_span().events

    # should create a new span (but return the original value)
    assert app() == "c"
    assert SpansContainer.get_span().events


def test_wrapping_with_logging_override_default_usage(caplog):
    @lumigo_tracer(enhance_print=True)
    def lambda_test_function(event, context):
        logging.warning("hello\nworld")
        return 1

    assert lambda_test_function({}, SimpleNamespace(aws_request_id="1234")) == 1
    assert utils._ENHANCE_PRINT is True
    assert "RequestId: 1234 test_sync_hook.py " in caplog.text
    assert " WARNING  hello\nRequestId: 1234 world" in caplog.text


def test_wrapping_with_logging_override_complex_usage():
    @lumigo_tracer(enhance_print=True)
    def lambda_test_function(event, context):
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(name)s [%(levelname)s] %(message)s")  # Format of a client.
        handler.setFormatter(formatter)

        log = logging.getLogger("my_test")
        log.propagate = False
        log.handlers = [handler]
        log.setLevel("INFO")

        log.info("hello\nworld")
        return 1

    with CaptureOutput() as capturer:
        assert lambda_test_function({}, SimpleNamespace(aws_request_id="1234")) == 1
        assert utils._ENHANCE_PRINT is True
        assert "RequestId: 1234 my_test [INFO] hello" in capturer.get_lines()
        assert "RequestId: 1234 world" in capturer.get_lines()


def test_wrapping_without_logging_override(caplog):
    @lumigo_tracer()
    def lambda_test_function(event, context):
        logging.warning("hello\nworld")
        return 1

    assert lambda_test_function({}, SimpleNamespace(aws_request_id="1234")) == 1
    assert utils._ENHANCE_PRINT is False
    assert " WARNING  hello\nworld" in caplog.text
