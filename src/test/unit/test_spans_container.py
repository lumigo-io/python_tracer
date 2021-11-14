import copy
import os

import mock
import inspect
import json
import uuid
from datetime import datetime

import pytest

from lumigo_tracer import lumigo_utils
from lumigo_tracer.wrappers.http.http_parser import HTTP_TYPE
from lumigo_tracer.spans_container import (
    SpansContainer,
    TimeoutMechanism,
    FUNCTION_TYPE,
    MALFORMED_TXID,
)
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


@pytest.mark.dont_mock_lumigo_utils_reporter
def test_start(monkeypatch):
    lumigo_utils_mock = mock.Mock()
    monkeypatch.setenv("LUMIGO_USE_TRACER_EXTENSION", "true")
    monkeypatch.setattr(lumigo_utils, "write_extension_file", lumigo_utils_mock)
    monkeypatch.setattr(SpansContainer, "_generate_start_span", lambda *args, **kwargs: {"a": "a"})
    monkeypatch.setattr(Configuration, "should_report", True)
    SpansContainer().start()
    lumigo_utils_mock.assert_called_once_with([{"a": "a"}], "span")


def test_spans_container_end_function_got_none_return_value(monkeypatch):
    SpansContainer.create_span()
    SpansContainer.get_span().start()
    SpansContainer.get_span().end(None)
    assert SpansContainer.get_span().function_span["return_value"] is None


def test_spans_container_end_function_not_send_spans_on_send_only_on_errors_mode(
    monkeypatch, dummy_span, tmpdir
):
    monkeypatch.setenv("LUMIGO_USE_TRACER_EXTENSION", "TRUE")
    reported_ttl, stop_path_path = only_if_error(dummy_span, monkeypatch, tmpdir)
    stop_file_content = json.loads(open(stop_path_path, "r").read())
    assert json.dumps(stop_file_content) == json.dumps([{}])
    assert reported_ttl is None


def test_spans_container_end_shoudnt_create_file_if_not_using_extension(
    monkeypatch, dummy_span, tmpdir
):
    reported_ttl, stop_path_path = only_if_error(dummy_span, monkeypatch, tmpdir)
    assert os.path.isfile(stop_path_path) is False
    assert reported_ttl is None


def only_if_error(dummy_span, monkeypatch, tmpdir):
    extension_dir = tmpdir.mkdir("tmp")
    monkeypatch.setenv("LUMIGO_EXTENSION_SPANS_DIR_KEY", extension_dir)
    monkeypatch.setattr(uuid, "uuid4", lambda *args, **kwargs: "span_name")
    Configuration.send_only_if_error = True
    SpansContainer.create_span()
    SpansContainer.get_span().start()
    SpansContainer.get_span().add_span(dummy_span)
    reported_ttl = SpansContainer.get_span().end({})
    stop_path_path = f"{lumigo_utils.get_extension_dir()}/span_name_stop"
    return reported_ttl, stop_path_path


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


def test_spans_container_end_function_with_error_double_size_limit(monkeypatch, dummy_span):
    long_string = "v" * int(Configuration.get_max_entry_size() * 1.5)
    monkeypatch.setenv("LONG_STRING", long_string)
    event = {"k": long_string}
    SpansContainer.create_span(event)
    SpansContainer.get_span().start()
    start_span = copy.deepcopy(SpansContainer.get_span().function_span)
    SpansContainer.get_span().add_exception_event(Exception("Some Error"), inspect.trace())

    SpansContainer.get_span().end(event=event)

    end_span = SpansContainer.get_span().function_span
    assert len(end_span["event"]) > len(start_span["event"])
    assert end_span["event"] == json.dumps(event)


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


def test_get_patched_root(monkeypatch, context):
    monkeypatch.setenv(
        "_X_AMZN_TRACE_ID",
        "Root=1-5fd891b8-252f5de90a085ae04267aa4e;Parent=0a885f800de045d4;Sampled=0",
    )
    SpansContainer.create_span({}, context)
    result = SpansContainer.get_span().get_patched_root()
    root = result.split(";")[0].split("=")[1]
    one, current_time, txid = root.split("-")

    result_time = datetime.fromtimestamp(int(current_time, 16))
    assert one == "1"
    assert (result_time - datetime.now()).total_seconds() < 5
    assert txid == "252f5de90a085ae04267aa4e"


def test_malformed_txid(monkeypatch, context):
    monkeypatch.setenv(
        "_X_AMZN_TRACE_ID", f"Root=1-5fd891b8-{MALFORMED_TXID};Parent=0a885f800de045d4;Sampled=0"
    )
    SpansContainer.create_span({}, context)

    assert SpansContainer.get_span().transaction_id != MALFORMED_TXID
    assert SpansContainer.get_span().function_span["isMalformedTransactionId"]
    result = SpansContainer.get_span().get_patched_root()
    output_trace_id = result.split(";")[0].split("=")[1].split("-")[2]
    assert output_trace_id == SpansContainer.get_span().transaction_id
