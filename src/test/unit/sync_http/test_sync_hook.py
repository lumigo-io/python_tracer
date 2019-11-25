import json
import time

import os
import sys
import urllib
from functools import wraps
from io import BytesIO
from types import SimpleNamespace
import logging

import urllib3
from capturer import CaptureOutput
from lumigo_tracer import lumigo_tracer, LumigoChalice, utils
from lumigo_tracer.parsers.parser import Parser
import http.client
from lumigo_tracer.utils import (
    Configuration,
    STEP_FUNCTION_UID_KEY,
    LUMIGO_EVENT_KEY,
    _create_request_body,
)
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
    function_span = SpansContainer.get_span().function_span
    assert not SpansContainer.get_span().http_spans
    assert "started" in function_span
    assert "ended" in function_span
    assert reporter_mock.call_count == 2
    first_send = reporter_mock.call_args_list[0][1]["msgs"]
    assert len(first_send) == 1
    assert first_send[0]["id"].endswith("_started")
    assert first_send[0]["maxFinishTime"]


@pytest.mark.parametrize("exc", [ValueError("Oh no"), ValueError(), ValueError(Exception())])
def test_lambda_wrapper_exception(exc):
    @lumigo_tracer(token="123")
    def lambda_test_function():
        a = "A"  # noqa
        raise exc

    try:
        lambda_test_function()
    except ValueError:
        pass
    else:
        assert False

    function_span = SpansContainer.get_span().function_span
    assert not SpansContainer.get_span().http_spans
    assert function_span.get("error", {}).get("type") == "ValueError"
    # Make sure no lumigo_tracer
    assert len(function_span["error"]["frames"]) == 1
    assert function_span["error"]["frames"][0].pop("lineno") > 0
    assert function_span["error"]["frames"][0] == {
        "function": "lambda_test_function",
        "fileName": __file__,
        "variables": {"a": "A", "exc": str(exc)},
    }
    assert not function_span["id"].endswith("_started")
    assert "reporter_rtt" in function_span
    assert "maxFinishTime" not in function_span
    # Test that we can create an output message out of this span
    assert _create_request_body([function_span], prune_size_flag=False)


def test_lambda_wrapper_http():
    @lumigo_tracer(token="123")
    def lambda_test_function():
        time.sleep(0.01)
        http.client.HTTPConnection("www.google.com").request("POST", "/")

    lambda_test_function()
    http_spans = SpansContainer.get_span().http_spans
    assert http_spans
    assert http_spans[0].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "started" in http_spans[0]
    assert http_spans[0]["started"] > SpansContainer.get_span().function_span["started"]
    assert "ended" in http_spans[0]
    assert "Content-Length" in http_spans[0]["info"]["httpInfo"]["request"]["headers"]


def test_lambda_wrapper_query_with_http_params():
    @lumigo_tracer(token="123")
    def lambda_test_function():
        http.client.HTTPConnection("www.google.com").request("GET", "/?q=123")

    lambda_test_function()
    http_spans = SpansContainer.get_span().http_spans

    assert http_spans
    print(http_spans[0]["info"]["httpInfo"]["request"])
    assert http_spans[0]["info"]["httpInfo"]["request"]["uri"] == "www.google.com/?q=123"


def test_lambda_wrapper_get_response():
    @lumigo_tracer(token="123")
    def lambda_test_function():
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("GET", "")
        conn.getresponse()

    lambda_test_function()
    http_spans = SpansContainer.get_span().http_spans

    assert http_spans
    assert http_spans[0]["info"]["httpInfo"]["response"]["statusCode"] == 200


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
    http_spans = SpansContainer.get_span().http_spans
    assert http_spans
    assert http_spans[0]["info"]["httpInfo"]["request"]["body"] == "123456"
    assert "Content-Length" in http_spans[0]["info"]["httpInfo"]["request"]["headers"]


def test_lambda_wrapper_no_headers():
    @lumigo_tracer(token="123")
    def lambda_test_function():
        http.client.HTTPConnection("www.google.com").send(BytesIO(b"123"))

    lambda_test_function()
    http_events = SpansContainer.get_span().http_spans
    assert len(http_events) == 1
    assert http_events[0].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "started" in http_events[0]
    assert "ended" in http_events[0]


def test_lambda_wrapper_http_non_splitted_send():
    @lumigo_tracer(token="123")
    def lambda_test_function():
        http.client.HTTPConnection("www.google.com").request("POST", "/")
        http.client.HTTPConnection("www.github.com").send(BytesIO(b"123"))

    lambda_test_function()
    http_events = SpansContainer.get_span().http_spans
    assert len(http_events) == 2


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
    assert Configuration.should_report == "123"


def test_bad_domains_scrubber(monkeypatch):
    monkeypatch.setenv("LUMIGO_DOMAINS_SCRUBBER", '["bad json')

    @lumigo_tracer(token="123", should_report=True)
    def lambda_test_function():
        pass

    lambda_test_function()
    assert utils.Configuration.should_report is False


def test_domains_scrubber_happy_flow(monkeypatch):
    @lumigo_tracer(token="123", domains_scrubber=[".*google.*"])
    def lambda_test_function():
        return http.client.HTTPConnection(host="www.google.com").send(b"\r\n")

    lambda_test_function()
    http_events = SpansContainer.get_span().http_spans
    assert len(http_events) == 1
    assert http_events[0].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "headers" not in http_events[0]["info"]["httpInfo"]["request"]
    assert http_events[0]["info"]["httpInfo"]["request"]["body"] == "The data is not available"


def test_domains_scrubber_override_allows_default_domains(monkeypatch):
    ssm_url = "www.ssm.123.amazonaws.com"

    @lumigo_tracer(token="123", domains_scrubber=[".*google.*"])
    def lambda_test_function():
        try:
            return http.client.HTTPConnection(host=ssm_url).send(b"\r\n")
        except Exception:
            return

    lambda_test_function()
    http_events = SpansContainer.get_span().http_spans
    assert len(http_events) == 1
    assert http_events[0].get("info", {}).get("httpInfo", {}).get("host") == ssm_url
    assert http_events[0]["info"]["httpInfo"]["request"]["headers"]


def test_wrapping_with_print_override():
    @lumigo_tracer(enhance_print=True)
    def lambda_test_function(event, context):
        print("hello\nworld")
        return 1

    with CaptureOutput() as capturer:
        assert lambda_test_function({}, SimpleNamespace(aws_request_id="1234")) == 1
        assert Configuration.enhanced_print is True
        assert "RequestId: 1234 hello" in capturer.get_lines()
        assert "RequestId: 1234 world" in capturer.get_lines()


def test_wrapping_without_print_override():
    @lumigo_tracer()
    def lambda_test_function(event, context):
        print("hello")
        return 1

    with CaptureOutput() as capturer:
        assert lambda_test_function({}, SimpleNamespace(aws_request_id="1234")) == 1
        assert Configuration.enhanced_print is False
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
    http_events = SpansContainer.get_span().http_spans
    assert any(
        '"Content-Type": "application/json"'
        in event.get("info", {}).get("httpInfo", {}).get("request", {}).get("headers", "")
        for event in http_events
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
    assert not SpansContainer._span

    # should create a new span (but return the original value)
    assert app() == "c"
    assert SpansContainer._span


def test_lumigo_chalice_create_extra_lambdas(monkeypatch):
    # mimic aws env
    monkeypatch.setitem(os.environ, "LAMBDA_RUNTIME_DIR", "true")

    class Chalice:
        """
        This class in a mimic of chalice.
        """

        touched = False

        @staticmethod
        def on_s3_event(**kwargs):
            Chalice.touched = True  # represents chalice's global analysis (in the deploy process)

            def _create_registration_function(func):
                @wraps(func)
                def user_lambda_handler(*args, **kwargs):
                    return func(*args, **kwargs)

                return user_lambda_handler

            return _create_registration_function

    app = Chalice()
    app = LumigoChalice(app)

    @app.on_s3_event(name="test")
    def handler(event, context):
        return "hello world"

    # should run the outer code before lambda execution, but not create span (in global execution)
    assert app.touched
    assert not SpansContainer._span

    # should create a new span (but return the original value)
    assert handler({}, {}) == "hello world"
    assert SpansContainer._span


def test_wrapping_with_logging_override_default_usage(caplog):
    @lumigo_tracer(enhance_print=True)
    def lambda_test_function(event, context):
        logging.warning("hello\nworld")
        return 1

    assert lambda_test_function({}, SimpleNamespace(aws_request_id="1234")) == 1
    assert Configuration.enhanced_print is True
    assert any("RequestId: 1234" in line and "hello" in line for line in caplog.text.split("\n"))
    assert any("RequestId: 1234" in line and "world" in line for line in caplog.text.split("\n"))


def test_wrapping_with_logging_exception(caplog):
    @lumigo_tracer(enhance_print=True)
    def lambda_test_function(event, context):
        logger = logging.getLogger("logger_name")
        handler = logging.StreamHandler()
        logger.addHandler(handler)

        try:
            1 / 0
        except Exception:  # You must call the logging.exception method just inside the except part.
            logger.exception("hello")
        return 1

    assert lambda_test_function({}, SimpleNamespace(aws_request_id="1234")) == 1
    #  Check all lines have exactly one RequestId.
    for line in caplog.text.splitlines():
        assert line.startswith("RequestId: 1234") and line.count("RequestId: 1234") == 1
    #  Check the message was logged.
    test_message = [line for line in caplog.text.splitlines() if line.endswith("hello")][0].replace(
        " ", ""
    )
    assert "ERROR" in test_message and "hello" in test_message


def test_wrapping_with_logging_override_complex_usage():
    @lumigo_tracer(enhance_print=True)
    def lambda_test_function(event, context):
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(name)s [%(levelname)s] %(message)s")  # Format of a client.
        handler.setFormatter(formatter)
        logger = logging.getLogger("my_test")
        logger.handlers = [handler]
        logger.setLevel("INFO")

        logger.info("hello\nworld")
        return 1

    with CaptureOutput() as capturer:
        assert lambda_test_function({}, SimpleNamespace(aws_request_id="1234")) == 1
        assert Configuration.enhanced_print is True
        assert "RequestId: 1234 my_test [INFO] hello" in capturer.get_lines()
        assert "RequestId: 1234 world" in capturer.get_lines()


def test_wrapping_without_logging_override(caplog):
    @lumigo_tracer()
    def lambda_test_function(event, context):
        logging.warning("hello\nworld")
        return 1

    assert lambda_test_function({}, SimpleNamespace(aws_request_id="1234")) == 1
    assert Configuration.enhanced_print is False
    assert any(
        "RequestId: 1234" not in line and "world" in line for line in caplog.text.split("\n")
    )
    assert any(
        "RequestId: 1234" not in line and "hello" in line for line in caplog.text.split("\n")
    )


def test_wrapping_urlib_stream_get():
    """
    This is the same case as the one of `requests.get`.
    """

    @lumigo_tracer()
    def lambda_test_function(event, context):
        r = urllib3.PoolManager().urlopen("GET", "https://www.google.com", preload_content=False)
        return b"".join(r.stream(32))

    lambda_test_function({}, None)
    assert len(SpansContainer.get_span().http_spans) == 1
    event = SpansContainer.get_span().http_spans[0]
    assert event["info"]["httpInfo"]["response"]["body"]
    assert event["info"]["httpInfo"]["response"]["statusCode"] == 200
    assert event["info"]["httpInfo"]["host"] == "www.google.com"


@pytest.mark.parametrize(
    "event, expected_triggered_by, expected_message_id",
    [
        ({}, "unknown", None),
        ({"result": 1, LUMIGO_EVENT_KEY: {STEP_FUNCTION_UID_KEY: "123"}}, "stepFunction", "123"),
    ],
)
def test_wrapping_step_function(event, expected_triggered_by, expected_message_id):
    @lumigo_tracer(step_function=True)
    def lambda_test_function(event, context):
        return {"result": 1}

    lambda_test_function(event, None)
    span = SpansContainer.get_span()
    assert len(span.http_spans) == 1
    assert span.function_span["info"]["triggeredBy"] == expected_triggered_by
    assert span.function_span["info"].get("messageId") == expected_message_id
    return_value = json.loads(span.function_span["return_value"])
    assert return_value["result"] == 1
    assert return_value[LUMIGO_EVENT_KEY][STEP_FUNCTION_UID_KEY]
    assert span.http_spans[0]["info"]["httpInfo"]["host"] == "StepFunction"


def test_omitting_keys():
    @lumigo_tracer()
    def lambda_test_function(event, context):
        d = {"a": "b", "myPassword": "123"}
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("POST", "/", json.dumps(d))
        return {"secret_password": "lumigo rulz"}

    lambda_test_function({"key": "24"}, None)
    span = SpansContainer.get_span()
    assert span.function_span["return_value"] == '{"secret_password": "****"}'
    assert span.function_span["event"] == '{"key": "****"}'
    assert SpansContainer.get_span().http_spans[0]["info"]["httpInfo"]["request"][
        "body"
    ] == json.dumps({"a": "b", "myPassword": "****"})
