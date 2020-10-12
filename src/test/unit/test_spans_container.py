import inspect

import pytest

from lumigo_tracer.wrappers.http.http_parser import HTTP_TYPE
from lumigo_tracer.spans_container import SpansContainer, TimeoutMechanism, FUNCTION_TYPE
from lumigo_tracer.lumigo_utils import Configuration, EXECUTION_TAGS_KEY


@pytest.fixture
def dummy_span():
    return {"id": "span1", "type": "http", "info": {"hello": "world"}}


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
    monkeypatch, dummy_span
):
    Configuration.send_only_if_error = True

    SpansContainer.create_span()
    SpansContainer.get_span().start()

    SpansContainer.get_span().add_span(dummy_span)

    reported_ttl = SpansContainer.get_span().end({})
    assert reported_ttl is None


def test_spans_container_end_function_send_spans_on_send_only_on_errors_mode(
    monkeypatch, dummy_span
):
    Configuration.send_only_if_error = True

    SpansContainer.create_span()
    SpansContainer.get_span().start()

    SpansContainer.get_span().add_span(dummy_span)
    try:
        1 / 0
    except Exception:
        SpansContainer.get_span().add_exception_event(Exception("Some Error"), inspect.trace())

    reported_ttl = SpansContainer.get_span().end({})
    assert reported_ttl is not None


def test_spans_container_end_function_send_only_on_errors_mode_false_not_effecting(
    monkeypatch, dummy_span
):

    SpansContainer.create_span()
    SpansContainer.get_span().start()

    SpansContainer.get_span().add_span(dummy_span)

    reported_ttl = SpansContainer.get_span().end({})
    assert reported_ttl is not None


def test_spans_container_timeout_mechanism_send_only_on_errors_mode(
    monkeypatch, context, reporter_mock, dummy_span
):
    monkeypatch.setattr(Configuration, "send_only_if_error", True)

    SpansContainer.create_span()
    SpansContainer.get_span().start()
    SpansContainer.get_span().add_span(dummy_span)

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


def test_timeout_mechanism_timeout_occurred_doesnt_send_span_twice(
    monkeypatch, context, dummy_span
):
    SpansContainer.create_span()
    SpansContainer.get_span().start(context=context)
    SpansContainer.get_span().add_span(dummy_span)

    assert SpansContainer.get_span().span_ids_to_send
    SpansContainer.get_span().handle_timeout()
    assert not SpansContainer.get_span().span_ids_to_send


def test_timeout_mechanism_timeout_occurred_send_new_spans(monkeypatch, context, dummy_span):
    SpansContainer.create_span()
    SpansContainer.get_span().start(context=context)
    SpansContainer.get_span().add_span(dummy_span)
    SpansContainer.get_span().handle_timeout()

    SpansContainer.get_span().add_span(dummy_span)
    assert SpansContainer.get_span().span_ids_to_send


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


def test_get_span_by_id():
    container = SpansContainer.get_span()
    container.add_span({"id": 1, "extra": "a"})
    container.add_span({"id": 2, "extra": "b"})
    container.add_span({"id": 3, "extra": "c"})
    assert SpansContainer.get_span().get_span_by_id(2)["extra"] == "b"
    assert SpansContainer.get_span().get_span_by_id(5) is None
