import pytest

from lumigo_tracer.parsers.http_data_classes import HttpRequest
from lumigo_tracer.spans_container import SpansContainer, TimeoutMechanism, TIMEOUT_BUFFER
from lumigo_tracer import spans_container
from lumigo_tracer import utils
from lumigo_tracer.utils import Configuration


@pytest.fixture()
def dummy_http_request():
    return HttpRequest(
        host="dummy", method="dummy", uri="dummy", headers={"dummy": "dummy"}, body="dummy"
    )


@pytest.fixture(autouse=True)
def mock_report_json(monkeypatch):
    monkeypatch.setattr(utils, "report_json", lambda *args, **kwargs: 1)


def _is_start_span_sent():
    return SpansContainer.get_span().function_span.get("reporter_rtt") is not None


def test_spans_container_send_only_on_errors_mode_false_not_effecting(monkeypatch):
    SpansContainer.create_span()
    SpansContainer.get_span().start()
    assert _is_start_span_sent() is True


def test_spans_container_not_send_start_span_on_send_only_on_errors_mode(monkeypatch):
    monkeypatch.setattr(spans_container, "SEND_ONLY_IF_ERROR", True)

    SpansContainer.create_span()
    SpansContainer.get_span().start()
    assert _is_start_span_sent() is False


def test_spans_container_end_function_not_send_spans_on_send_only_on_errors_mode(
    monkeypatch, dummy_http_request
):
    monkeypatch.setattr(spans_container, "SEND_ONLY_IF_ERROR", True)

    SpansContainer.create_span()
    SpansContainer.get_span().start()

    SpansContainer.get_span().add_request_event(dummy_http_request)

    reported_ttl = SpansContainer.get_span().end({})
    assert reported_ttl is None


def test_spans_container_end_function_send_spans_on_send_only_on_errors_mode(
    monkeypatch, dummy_http_request
):
    monkeypatch.setattr(spans_container, "SEND_ONLY_IF_ERROR", True)

    SpansContainer.create_span()
    SpansContainer.get_span().start()

    SpansContainer.get_span().add_request_event(dummy_http_request)
    SpansContainer.get_span().add_exception_event(Exception("Some Error"))

    reported_ttl = SpansContainer.get_span().end({})
    assert reported_ttl is not None


def test_spans_container_end_function_send_only_on_errors_mode_false_not_effecting(
    monkeypatch, dummy_http_request
):

    SpansContainer.create_span()
    SpansContainer.get_span().start()

    SpansContainer.get_span().add_request_event(dummy_http_request)

    reported_ttl = SpansContainer.get_span().end({})
    assert reported_ttl is not None


def test_timeout_mechanism_disabled_by_configuration(monkeypatch, context):
    monkeypatch.setattr(Configuration, "timeout_timer", False)
    SpansContainer.create_span()
    SpansContainer.get_span().start()

    assert not TimeoutMechanism.is_activated()


def test_timeout_mechanism_too_short_time(monkeypatch, context):
    monkeypatch.setattr(Configuration, "timeout_timer", True)
    monkeypatch.setattr(context, "get_remaining_time_in_millis", lambda: TIMEOUT_BUFFER / 2)
    SpansContainer.create_span()
    SpansContainer.get_span().start(context=context)

    assert not TimeoutMechanism.is_activated()


def test_timeout_mechanism_timeout_occurred_doesnt_send_span_twice(monkeypatch, context):
    SpansContainer.create_span()
    SpansContainer.get_span().start(context=context)
    SpansContainer.get_span().add_request_event(
        HttpRequest(host="google.com", method="", uri="", headers=None, body="")
    )

    assert SpansContainer.get_span().http_span_ids_to_send
    SpansContainer.get_span().handle_timeout()
    assert not SpansContainer.get_span().http_span_ids_to_send


def test_timeout_mechanism_timeout_occurred_send_new_spans(monkeypatch, context):
    SpansContainer.create_span()
    SpansContainer.get_span().start(context=context)
    SpansContainer.get_span().add_request_event(
        HttpRequest(host="google.com", method="", uri="", headers=None, body="")
    )
    SpansContainer.get_span().handle_timeout()

    SpansContainer.get_span().add_request_event(
        HttpRequest(host="google.com", method="", uri="", headers=None, body="2")
    )
    assert SpansContainer.get_span().http_span_ids_to_send


def test_timeout_mechanism_timeout_occurred_send_updated_spans(monkeypatch, context):
    SpansContainer.create_span()
    SpansContainer.get_span().start(context=context)
    SpansContainer.get_span().add_request_event(
        HttpRequest(host="google.com", method="", uri="", headers=None, body="")
    )
    SpansContainer.get_span().handle_timeout()

    SpansContainer.get_span().update_event_response(
        host="google.com", status_code=200, headers=None, body=b"2"
    )
    assert SpansContainer.get_span().http_span_ids_to_send
