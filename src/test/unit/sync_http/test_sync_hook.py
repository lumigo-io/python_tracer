import os

from lumigo_tracer import lumigo_tracer
import http.client
from lumigo_tracer import utils

from lumigo_tracer.spans_container import SpansContainer


def events_by_mock(reporter_mock):
    return reporter_mock.call_args[1]["msgs"]


def test_lambda_wrapper_basic_events(reporter_mock):
    """
    This test checks that the basic events (start and end messages) has been sent.
    """

    @lumigo_tracer
    def lambda_test_function():
        pass

    lambda_test_function()
    events = events_by_mock(reporter_mock)
    assert len(events) == 1
    assert "started" in events[0]
    assert "ended" in events[0]


def test_lambda_wrapper_exception(reporter_mock):
    @lumigo_tracer
    def lambda_test_function():
        raise ValueError("Oh no")

    try:
        lambda_test_function()
    except ValueError:
        pass
    else:
        assert False

    events = events_by_mock(reporter_mock)
    assert len(events) == 1
    assert events[0].get("error", "").startswith("ValueError")


def test_lambda_wrapper_http(reporter_mock):
    @lumigo_tracer
    def lambda_test_function():
        http.client.HTTPConnection("www.google.com").request("POST", "/")

    lambda_test_function()
    events = events_by_mock(reporter_mock)
    assert len(events) == 2
    assert events[1].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"


def test_kill_switch(monkeypatch):
    monkeypatch.setattr(os, "environ", {"LUMIGO_SWITCH_OFF": 1})

    @lumigo_tracer
    def lambda_test_function():
        return 1

    assert lambda_test_function() == 1
    assert not SpansContainer._span


def test_wrapping_exception(monkeypatch):
    monkeypatch.setattr(SpansContainer, "create_span", lambda x: 1 / 0)

    @lumigo_tracer
    def lambda_test_function():
        return 1

    assert lambda_test_function() == 1
    assert not SpansContainer._span


def test_wrapping_with_parameters(monkeypatch):
    monkeypatch.setattr(SpansContainer, "create_span", lambda x: 1 / 0)

    @lumigo_tracer(should_report="123")
    def lambda_test_function():
        return 1

    assert lambda_test_function() == 1
    assert utils.SHOULD_REPORT == "123"
