import pytest
from lumigo_tracer.utils import _create_request_body, _is_span_has_error, _get_event_base64_size
import json


@pytest.fixture
def dummy_span():
    return {"dummy": "dummy"}


@pytest.fixture
def function_end_span():
    return {"dummy_end": "dummy_end"}


@pytest.fixture
def error_span():
    return {"dummy": "dummy", "error": "Error"}


@pytest.fixture
def status_code_error_span():
    return {"dummy": "dummy", "info": {"httpInfo": {"response": {"statusCode": 500}}}}


@pytest.fixture
def status_code_span():
    return {"dummy": "dummy", "info": {"httpInfo": {"response": {"statusCode": 200}}}}


def test_is_span_has_error(dummy_span, error_span, status_code_error_span, status_code_span):
    assert _is_span_has_error(dummy_span) is False
    assert _is_span_has_error(error_span) is True
    assert _is_span_has_error(status_code_span) is False
    assert _is_span_has_error(status_code_error_span) is True


def test_create_request_body_default(dummy_span):
    assert _create_request_body(dummy_span, False) == json.dumps(dummy_span)


def test_create_request_body_not_not_effecting_small_events(dummy_span):
    assert _create_request_body([dummy_span], True, 1_000_000) == json.dumps([dummy_span])


def test_create_request_body_keep_function_span(dummy_span, function_end_span):
    expected_result = [dummy_span, dummy_span, dummy_span, function_end_span]
    size = _get_event_base64_size(expected_result)
    assert _create_request_body(expected_result * 2, True, size) == json.dumps(
        [dummy_span, dummy_span, dummy_span, function_end_span]
    )


def test_create_request_body_take_error_first(dummy_span, error_span, function_end_span):
    expected_result = [dummy_span, dummy_span, error_span, function_end_span]
    input = [
        dummy_span,
        dummy_span,
        dummy_span,
        dummy_span,
        dummy_span,
        error_span,
        function_end_span,
    ]
    size = _get_event_base64_size(expected_result)
    assert _create_request_body(input, True, size) == json.dumps(
        [error_span, dummy_span, dummy_span, function_end_span]
    )
