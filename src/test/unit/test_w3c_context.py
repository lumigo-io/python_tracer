import pytest

from lumigo_tracer.w3c_context import (
    TRACEPARENT_HEADER_NAME,
    TRACESTATE_HEADER_NAME,
    add_w3c_trace_propagator,
    get_w3c_message_id,
    is_w3c_headers,
    should_skip_trace_propagation,
)


def test_add_w3c_trace_propagator_existing_header_happy_flow():
    headers = {
        TRACEPARENT_HEADER_NAME: "00-11111111111111111111111100000000-aaaaaaaaaaaaaaaa-01",
        TRACESTATE_HEADER_NAME: "old",
    }
    add_w3c_trace_propagator(headers, "111111111111112222222222")

    parts = headers[TRACEPARENT_HEADER_NAME].split("-")
    assert len(parts) == 4
    assert parts[0] == "00"
    assert parts[1] == "11111111111111111111111100000000"  # Not replacing the existing traceId
    assert parts[3] == "01"
    assert headers[TRACESTATE_HEADER_NAME] == f"old,lumigo={parts[2]}"


def test_add_w3c_trace_propagator_no_header_happy_flow():
    headers = {}
    add_w3c_trace_propagator(headers, "111111111111112222222222")

    parts = headers[TRACEPARENT_HEADER_NAME].split("-")
    assert len(parts) == 4
    assert parts[0] == "00"
    assert parts[1] == "11111111111111222222222200000000"
    assert parts[3] == "01"
    assert headers[TRACESTATE_HEADER_NAME] == f"lumigo={parts[2]}"


def test_add_w3c_trace_propagator_malformed_header():
    headers = {
        TRACEPARENT_HEADER_NAME: "something else-aaaaaaaaaaaaaaaa-01",
    }
    add_w3c_trace_propagator(headers, "111111111111112222222222")

    parts = headers[TRACEPARENT_HEADER_NAME].split("-")
    assert len(parts) == 4
    assert parts[0] == "00"
    assert parts[1] == "11111111111111222222222200000000"
    assert parts[3] == "01"
    assert headers[TRACESTATE_HEADER_NAME] == f"lumigo={parts[2]}"


@pytest.mark.parametrize(
    "headers, expected",
    [
        (
            {TRACEPARENT_HEADER_NAME: "00-11111111111111111111111100000000-aaaaaaaaaaaaaaaa-01"},
            "aaaaaaaaaaaaaaaa",
        ),
        ({TRACEPARENT_HEADER_NAME: "00-malformed-aaaaaaaaaaaaaaaa-01"}, None),
        ({}, None),
    ],
)
def test_get_w3c_message_id(headers, expected):
    assert get_w3c_message_id(headers) == expected


@pytest.mark.parametrize(
    "headers, expected",
    [
        (
            {TRACEPARENT_HEADER_NAME: "00-11111111111111111111111100000000-aaaaaaaaaaaaaaaa-01"},
            True,
        ),
        ({TRACEPARENT_HEADER_NAME: "00-malformed-aaaaaaaaaaaaaaaa-01"}, False),
        ({}, False),
    ],
)
def test_is_w3c_headers(headers, expected):
    assert is_w3c_headers(headers) == expected


@pytest.mark.parametrize(
    "headers, expected",
    [
        ({}, False),
        ({"another": "header"}, False),
        ({"x-amz-content-sha256": "123"}, True),
        ({"X-amz-content-SHA256": "123"}, True),
    ],
)
def test_should_skip_trace_propagation(headers, expected):
    assert should_skip_trace_propagation(headers) == expected


def test_dont_add_header_for_skipped_context():
    headers = {"x-amz-content-sha256": "123"}
    add_w3c_trace_propagator(headers, "111111111111112222222222")

    assert headers == {"x-amz-content-sha256": "123"}
