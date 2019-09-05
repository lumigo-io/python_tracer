import pytest
from lumigo_tracer.utils import (
    _create_request_body,
    _is_span_has_error,
    _get_event_base64_size,
    MAX_VARS_SIZE,
    extract_frames_from_exception,
    _truncate_locals,
    MAX_VAR_LEN,
    prepare_large_data,
)
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


@pytest.mark.parametrize(
    ("input_span", "expected_is_error"),
    [
        ({"dummy": "dummy"}, False),
        ({"dummy": "dummy", "error": "Error"}, True),
        ({"dummy": "dummy", "info": {"httpInfo": {"response": {"statusCode": 500}}}}, True),
        ({"dummy": "dummy", "info": {"httpInfo": {"response": {"statusCode": 200}}}}, False),
    ],
)
def test_is_span_has_error(input_span, expected_is_error):
    assert _is_span_has_error(input_span) is expected_is_error


def test_create_request_body_default(dummy_span):
    assert _create_request_body([dummy_span], False) == json.dumps([dummy_span])


def test_create_request_body_not_effecting_small_events(dummy_span):
    assert _create_request_body([dummy_span], True, 1_000_000) == json.dumps([dummy_span])


def test_create_request_body_keep_function_span(dummy_span, function_end_span):
    expected_result = [dummy_span, dummy_span, dummy_span, function_end_span]
    size = _get_event_base64_size(expected_result)
    assert _create_request_body(expected_result * 2, True, size) == json.dumps(
        [function_end_span, dummy_span, dummy_span, dummy_span]
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
        [function_end_span, error_span, dummy_span, dummy_span]
    )


@pytest.mark.parametrize(
    ("f_locals", "expected"),
    [
        ({}, {}),  # Empty.
        ({"a": "b"}, {"a": "b"}),  # Short.
        ({"a": "b" * (MAX_VAR_LEN + 1)}, {"a": "b" * MAX_VAR_LEN + "...[too long]"}),  # Long.
        # Some short, some long.
        (
            {"l1": "l" * (MAX_VAR_LEN + 1), "s1": "s", "l2": "l" * (MAX_VAR_LEN + 1), "s2": "s"},
            {
                "l1": "l" * MAX_VAR_LEN + "...[too long]",
                "s1": "s",
                "l2": "l" * MAX_VAR_LEN + "...[too long]",
                "s2": "s",
            },
        ),
        ({"a": ["b", "c"]}, {"a": str(["b", "c"])}),  # Not str.
    ],
)
def test_frame_truncate_locals(f_locals, expected):
    assert _truncate_locals(f_locals, MAX_VARS_SIZE) == expected


def test_frame_truncate_locals_pass_max_vars_size():
    f_locals = {i: "i" for i in range(MAX_VARS_SIZE * 2)}
    actual = _truncate_locals(f_locals, MAX_VARS_SIZE)
    assert len(actual) < MAX_VARS_SIZE
    assert len(actual) > 0


def test_extract_frames_from_exception():
    def func_a():
        a = "A"  # noqa
        func_b()

    def func_b():
        one = 1
        zero = 0
        one / zero

    try:
        e = "E"  # noqa
        func_a()
    except Exception:
        frames = extract_frames_from_exception()

    assert frames[0]["function"] == "func_b"
    assert frames[0]["variables"] == {"one": "1", "zero": "0"}
    assert frames[1]["function"] == "func_a"
    assert frames[1]["variables"]["a"] == "A"
    assert frames[2]["function"] == "test_extract_frames_from_exception"
    assert frames[2]["variables"]["e"] == "E"


def test_extract_frames_from_exception__max_recursion():
    def func():
        a = "A"  # noqa
        func()

    try:
        e = "E"  # noqa
        func()
    except RecursionError:
        frames = extract_frames_from_exception()

    assert frames[0]["function"] == "func"


def test_extract_frames_from_exception__pass_max_vars_size():
    def func():
        for i in range(MAX_VARS_SIZE * 2):
            exec(f"a{i} = 'A'")
        1 / 0

    try:
        func()
    except Exception:
        frames = extract_frames_from_exception()

    assert len(frames[0]["variables"]) < MAX_VARS_SIZE
    assert len(frames[0]["variables"]) > 0


def test_extract_frames_from_exception__huge_var():
    try:
        a = "A" * MAX_VARS_SIZE  # noqa F841
        1 / 0
    except Exception:
        frames = extract_frames_from_exception()

    assert frames[0]["variables"]["a"] == "A" * MAX_VAR_LEN + "...[too long]"


def test_extract_frames_from_exception__check_all_keys_and_values():
    def func():
        a = "A"  # noqa
        1 / 0

    try:
        func()
    except Exception:
        frames = extract_frames_from_exception()

    assert frames[0] == {
        "function": "func",
        "fileName": __file__,
        "variables": {"a": "A"},
        "lineno": frames[1]["lineno"] - 3,
    }


@pytest.mark.parametrize(
    ("value", "output"),
    [
        ("aa", "aa"),  # happy flow
        (None, "None"),  # same key twice
        ("a" * 21, "a" * 20 + "...[too long]"),
        ({"a": "a"}, '{"a": "a"}'),  # dict.
        # dict that can't be converted to json.
        ({"a": set()}, "{'a': set()}"),  # type: ignore
        (b"a", "a"),  # bytes that can be decoded.
        (b"\xff\xfea\x00", "b'\\xff\\xfea\\x00'"),  # bytes that can't be decoded.
    ],
)
def test_prepare_large_data(value, output):
    assert prepare_large_data(value, 20) == output
