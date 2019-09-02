import pytest
from lumigo_tracer.utils import (
    _create_request_body,
    _is_span_has_error,
    _get_event_base64_size,
    Frame,
    MAX_VARS_SIZE,
    extract_frames_from_exception,
)
import json


@pytest.fixture(autouse=True)
def restore_total_frames_size():
    yield
    Frame.total_frames_size = 0


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
    ("var", "expected"),
    [
        ("aaa", "aaa"),  # Short.
        ("a" * (Frame.MAX_VAR_LEN + 1), "a" * Frame.MAX_VAR_LEN + "..."),  # Long.
        (["a", "b"], str(["a", "b"])),  # Type not str.
    ],
)
def test_frame_truncate_var(var, expected):
    assert Frame._truncate_var(var) == expected


@pytest.mark.parametrize(
    ("f_locals", "expected"),
    [
        ({}, {}),  # Empty.
        ({"a": "b"}, {"a": "b"}),  # Short.
        ({"a": "b" * (Frame.MAX_VAR_LEN + 1)}, {"a": "b" * Frame.MAX_VAR_LEN + "..."}),  # Long.
        # Some short, some long.
        (
            {
                "l1": "l" * (Frame.MAX_VAR_LEN + 1),
                "s1": "s",
                "l2": "l" * (Frame.MAX_VAR_LEN + 1),
                "s2": "s",
            },
            {
                "l1": "l" * Frame.MAX_VAR_LEN + "...",
                "s1": "s",
                "l2": "l" * Frame.MAX_VAR_LEN + "...",
                "s2": "s",
            },
        ),
        ({"a": ["b", "c"]}, {"a": str(["b", "c"])}),  # Not str.
        ({"f": list}, {}),  # Function.
    ],
)
def test_frame_truncate_locals(f_locals, expected):
    assert Frame._truncate_locals(f_locals) == expected


def test_frame_truncate_locals_pass_max_vars_size():
    f_locals = {i: "i" for i in range(MAX_VARS_SIZE * 2)}
    actual = Frame._truncate_locals(f_locals)
    assert len(actual) < MAX_VARS_SIZE


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

    assert frames[0].function == "func_b"
    assert frames[0].variables == {"one": "1", "zero": "0"}
    assert frames[1].function == "func_a"
    assert frames[1].variables == {"a": "A"}
    assert frames[2].function == "test_extract_frames_from_exception"
    assert frames[2].variables == {"e": "E"}


def test_extract_frames_from_exception_pass_max_vars_size():
    def func():
        for i in range(MAX_VARS_SIZE * 2):
            exec(f"a{i} = 'A'")
        1 / 0

    try:
        func()
    except Exception:
        frames = extract_frames_from_exception()

    assert len(frames[0].variables) < MAX_VARS_SIZE
    assert len(frames[0].variables) > 0


def test_frame_to_dict():
    def func():
        a = "A"  # noqa
        1 / 0

    try:
        func()
    except Exception:
        frames = extract_frames_from_exception()

    assert frames[0].to_dict() == {
        "function": "func",
        "file_name": __file__,
        "variables": {"a": "A"},
        "lineno": frames[1].lineno - 3,
    }
