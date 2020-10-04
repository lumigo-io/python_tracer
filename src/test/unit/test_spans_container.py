import inspect

import pytest

from lumigo_tracer.parsers.http_data_classes import HttpRequest
from lumigo_tracer.parsers.http_parser import HTTP_TYPE
from lumigo_tracer.spans_container import SpansContainer, TimeoutMechanism, FUNCTION_TYPE
from lumigo_tracer.lumigo_utils import Configuration, EXECUTION_TAGS_KEY, DEFAULT_MAX_ENTRY_SIZE


@pytest.fixture()
def dummy_http_request():
    return HttpRequest(
        host="dummy", method="dummy", uri="dummy", headers={"dummy": "dummy"}, body="dummy"
    )


def _is_start_span_sent():
    return SpansContainer.get_span().function_span.get("reporter_rtt") is not None


def test_spans_container_send_only_on_errors_mode_false_not_effecting(monkeypatch):
    SpansContainer.create_span()
    SpansContainer.get_span().start()
    assert _is_start_span_sent() is True


def test_spans_container_not_send_start_span_on_send_only_on_errors_mode(monkeypatch):
    Configuration.send_only_if_error = True

    SpansContainer.create_span()
    SpansContainer.get_span().start()
    assert _is_start_span_sent() is False


def test_spans_container_end_function_got_none_return_value(monkeypatch):
    SpansContainer.create_span()
    SpansContainer.get_span().start()
    SpansContainer.get_span().end(None)
    assert SpansContainer.get_span().function_span["return_value"] is None


def test_spans_container_end_function_not_send_spans_on_send_only_on_errors_mode(
    monkeypatch, dummy_http_request
):
    Configuration.send_only_if_error = True

    SpansContainer.create_span()
    SpansContainer.get_span().start()

    SpansContainer.get_span().add_request_event(dummy_http_request)

    reported_ttl = SpansContainer.get_span().end({})
    assert reported_ttl is None


def test_spans_container_end_function_send_spans_on_send_only_on_errors_mode(
    monkeypatch, dummy_http_request
):
    Configuration.send_only_if_error = True

    SpansContainer.create_span()
    SpansContainer.get_span().start()

    SpansContainer.get_span().add_request_event(dummy_http_request)
    try:
        1 / 0
    except Exception:
        SpansContainer.get_span().add_exception_event(Exception("Some Error"), inspect.trace())

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


def test_spans_container_timeout_mechanism_send_only_on_errors_mode(
    monkeypatch, context, reporter_mock, dummy_http_request
):
    monkeypatch.setattr(Configuration, "send_only_if_error", True)

    SpansContainer.create_span()
    SpansContainer.get_span().start()
    SpansContainer.get_span().add_request_event(dummy_http_request)

    SpansContainer.get_span().handle_timeout()

    messages = reporter_mock.call_args.kwargs["msgs"]
    assert len(messages) == 2
    assert [m for m in messages if m["type"] == FUNCTION_TYPE and m["id"].endswith("_started")]
    assert [m for m in messages if m["type"] == HTTP_TYPE]


def test_timeout_mechanism_disabled_by_configuration(monkeypatch, context):
    monkeypatch.setattr(Configuration, "timeout_timer", False)
    SpansContainer.create_span()
    SpansContainer.get_span().start()

    assert not TimeoutMechanism.is_activated()


def test_timeout_mechanism_too_short_time(monkeypatch, context):
    monkeypatch.setattr(Configuration, "timeout_timer", True)
    monkeypatch.setattr(context, "get_remaining_time_in_millis", lambda: 1000)
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


def test_add_tag():
    key = "my_key"
    value = "my_value"
    SpansContainer.get_span().add_tag(key, value)
    assert SpansContainer.get_span().function_span[EXECUTION_TAGS_KEY] == [
        {"key": key, "value": value}
    ]


def test_get_tags_len():
    assert SpansContainer.get_span().get_tags_len() == 0
    SpansContainer.get_span().add_tag("k0", "v0")
    SpansContainer.get_span().add_tag("k1", "v1")
    assert SpansContainer.get_span().get_tags_len() == 2


def test_aggregating_response_body(dummy_http_request):
    """
    This test is here to validate that we're not leaking memory on aggregating response body.
    Unfortunately python doesn't give us better tools, so we must check the problematic member itself.
    """
    SpansContainer.create_span()
    SpansContainer.get_span().add_request_event(dummy_http_request)

    big_response_chunk = b"leak" * DEFAULT_MAX_ENTRY_SIZE
    for _ in range(10):
        SpansContainer.get_span().update_event_response(
            host=None, status_code=200, headers=None, body=big_response_chunk
        )
    assert len(SpansContainer.get_span().previous_response_body) <= len(big_response_chunk)
