import os

from lumigo_tracer import lumigo_tracer
import http.client
from lumigo_tracer import utils
import pytest

from lumigo_tracer.spans_container import SpansContainer


def test_lambda_wrapper_basic_events(reporter_mock):
    """
    This test checks that the basic events (start and end messages) has been sent.
    """

    @lumigo_tracer
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
    @lumigo_tracer
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
    assert events[0].get("error", "").startswith("ValueError")
    assert not events[0]["id"].endswith("_started")
    assert "maxFinishTime" not in events[0]


def test_lambda_wrapper_http():
    @lumigo_tracer
    def lambda_test_function():
        http.client.HTTPConnection("www.google.com").request("POST", "/")

    lambda_test_function()
    events = SpansContainer.get_span().events
    assert len(events) == 2
    assert events[1].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "started" in events[1]
    assert "ended" in events[1]


def test_kill_switch(monkeypatch):
    monkeypatch.setattr(os, "environ", {"LUMIGO_SWITCH_OFF": "true"})

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


def test_wrapping_with_parameters():
    @lumigo_tracer(should_report="123")
    def lambda_test_function():
        return 1

    assert lambda_test_function() == 1
    assert utils.SHOULD_REPORT == "123"
