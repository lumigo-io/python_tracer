import inspect
from collections import OrderedDict
from decimal import Decimal
import datetime
import http.client
from mock import Mock

import pytest
from lumigo_tracer.utils import (
    _create_request_body,
    _is_span_has_error,
    _get_event_base64_size,
    MAX_VARS_SIZE,
    format_frames,
    _truncate_locals,
    MAX_VAR_LEN,
    format_frame,
    omit_keys,
    config,
    Configuration,
    LUMIGO_SECRET_MASKING_REGEX,
    LUMIGO_SECRET_MASKING_REGEX_BACKWARD_COMP,
    get_omitting_regex,
    warn_client,
    WARN_CLIENT_PREFIX,
    SKIP_SCRUBBING_KEYS,
    get_timeout_buffer,
    lumigo_dumps,
    prepare_host,
    EDGE_PATH,
    report_json,
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


def test_create_request_body_keep_function_span_and_filter_other_spans(
    dummy_span, function_end_span
):
    expected_result = [dummy_span, dummy_span, dummy_span, function_end_span]
    size = _get_event_base64_size(expected_result)
    assert _create_request_body(expected_result * 2, True, size) == json.dumps(
        [function_end_span, dummy_span, dummy_span, dummy_span]
    )


def test_create_request_body_take_error_first(dummy_span, error_span, function_end_span):
    expected_result = [function_end_span, error_span, dummy_span, dummy_span]
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
    assert _create_request_body(input, True, size) == json.dumps(expected_result)


@pytest.mark.parametrize(
    ("f_locals", "expected"),
    [
        ({}, {}),  # Empty.
        ({"a": "b"}, {"a": '"b"'}),  # Short.
        (
            {"a": "b" * (MAX_VAR_LEN + 1)},
            {"a": '"' + "b" * (MAX_VAR_LEN - 1) + "...[too long]"},
        ),  # Long.
        # Some short, some long.
        (
            {"l1": "l" * (MAX_VAR_LEN + 1), "s1": "s", "l2": "l" * (MAX_VAR_LEN + 1), "s2": "s"},
            {
                "l1": '"' + "l" * (MAX_VAR_LEN - 1) + "...[too long]",
                "s1": '"s"',
                "l2": '"' + "l" * (MAX_VAR_LEN - 1) + "...[too long]",
                "s2": '"s"',
            },
        ),
        ({"a": ["b", "c"]}, {"a": '["b", "c"]'}),  # Not str.
    ],
)
def test_frame_truncate_locals(f_locals, expected):
    assert _truncate_locals(f_locals, MAX_VARS_SIZE) == expected


def test_frame_truncate_locals_pass_max_vars_size():
    f_locals = {i: "i" for i in range(MAX_VARS_SIZE * 2)}
    actual = _truncate_locals(f_locals, MAX_VARS_SIZE)
    assert len(actual) < MAX_VARS_SIZE
    assert len(actual) > 0


def test_format_frames():
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
        frames = format_frames(inspect.trace())

    assert frames[0]["function"] == "func_b"
    assert frames[0]["variables"] == {"one": "1", "zero": "0"}
    assert frames[1]["function"] == "func_a"
    assert frames[1]["variables"]["a"] == '"A"'
    assert frames[2]["function"] == "test_format_frames"
    assert frames[2]["variables"]["e"] == '"E"'


def test_format_frames__max_recursion():
    def func():
        a = "A"  # noqa
        func()

    try:
        e = "E"  # noqa
        func()
    except RecursionError:
        frames = format_frames(inspect.trace())

    assert frames[0]["function"] == "func"


def test_format_frames__pass_max_vars_size():
    def func():
        for i in range(MAX_VARS_SIZE * 2):
            exec(f"a{i} = 'A'")
        1 / 0

    try:
        func()
    except Exception:
        frames = format_frames(inspect.trace())

    assert len(frames[0]["variables"]) < MAX_VARS_SIZE
    assert len(frames[0]["variables"]) > 0


def test_format_frames__huge_var():
    try:
        a = "A" * MAX_VARS_SIZE  # noqa F841
        1 / 0
    except Exception:
        frames = format_frames(inspect.trace())

    assert frames[0]["variables"]["a"] == '"' + "A" * (MAX_VAR_LEN - 1) + "...[too long]"


def test_format_frames__check_all_keys_and_values():
    def func():
        a = "A"  # noqa
        1 / 0

    try:
        func()
    except Exception:
        frames = format_frames(inspect.trace())

    assert frames[0] == {
        "function": "func",
        "fileName": __file__,
        "variables": {"a": '"A"'},
        "lineno": frames[1]["lineno"] - 3,
    }


@pytest.mark.parametrize(
    ("value", "output"),
    [
        ("aa", '"aa"'),  # happy flow - string
        (None, "null"),  # happy flow - None
        ("a" * 101, '"' + "a" * 99 + "...[too long]"),  # simple long string
        ({"a": "a"}, '{"a": "a"}'),  # happy flow - dict
        ({"a": set([1])}, '{"a": "{1}"}'),  # dict that can't be converted to json
        (b"a", '"a"'),  # bytes that can be decoded
        (b"\xff\xfea\x00", "\"b'\\\\xff\\\\xfea\\\\x00'\""),  # bytes that can't be decoded
        ({1: Decimal(1)}, '{"1": 1.0}'),  # decimal should be serializeable  (like in AWS!)
        ({"key": "b"}, '{"key": "****"}'),  # simple omitting
        ({"a": {"key": "b"}}, '{"a": {"key": "****"}}'),  # inner omitting
        ({"a": {"key": "b" * 100}}, '{"a": {"key": "****"}}'),  # long omitting
        ({"a": "b" * 300}, f'{{"a": "{"b" * 93}...[too long]'),  # long key
        ('{"a": "b"}', '{"a": "b"}'),  # string which is a simple json
        ('{"a": {"key": "b"}}', '{"a": {"key": "****"}}'),  # string with inner omitting
        ("{1: ", '"{1: "'),  # string which is not json but look like one
        (b'{"password": "abc"}', '{"password": "****"}'),  # omit of bytes
        ({"a": '{"password": 123}'}, '{"a": "{\\"password\\": 123}"}'),  # ignore inner json-string
        ({None: 1}, '{"null": 1}'),
        ({"1": datetime.datetime(1994, 4, 22)}, '{"1": "1994-04-22 00:00:00"}'),
        (OrderedDict({"a": "b", "key": "123"}), '{"a": "b", "key": "****"}'),  # OrderedDict
        (  # Skip scrubbing
            {SKIP_SCRUBBING_KEYS[0]: {"password": 1}},
            f'{{"{SKIP_SCRUBBING_KEYS[0]}": {{"password": 1}}}}',
        ),
        ([{"password": 1}, {"a": "b"}], '[{"password": "****"}, {"a": "b"}]'),  # list of dicts
    ],
)
def test_lumigo_dumps(value, output):
    assert lumigo_dumps(value, max_size=100) == output


def test_lumigo_dumps_enforce_jsonify_raise_error():
    with pytest.raises(TypeError):
        assert lumigo_dumps({"a": set()}, max_size=100, enforce_jsonify=True)


def test_lumigo_dumps_no_regexes(monkeypatch):
    monkeypatch.setenv(LUMIGO_SECRET_MASKING_REGEX, "[]")
    result = lumigo_dumps({"key": "123"}, max_size=100, enforce_jsonify=True)
    assert result == '{"key": "123"}'


def test_lumigo_dumps_omit_keys_environment(monkeypatch):
    monkeypatch.setenv(LUMIGO_SECRET_MASKING_REGEX, '[".*evilPlan.*"]')
    value = {"password": "abc", "evilPlan": {"take": "over", "the": "world"}}
    assert lumigo_dumps(value, max_size=100) == '{"password": "abc", "evilPlan": "****"}'


def test_format_frame():
    try:
        a = "A"  # noqa F841
        password = "123"  # noqa F841
        1 / 0
    except Exception:
        frame_info = inspect.trace()[0]

    converted_frame = format_frame(frame_info, MAX_VARS_SIZE)
    variables = converted_frame.pop("variables")
    assert converted_frame == {
        "lineno": frame_info.lineno,
        "fileName": frame_info.filename,
        "function": frame_info.function,
    }
    assert variables["a"] == '"A"'
    assert variables["password"] == '"****"'


def test_get_omitting_regexes(monkeypatch):
    monkeypatch.setenv(LUMIGO_SECRET_MASKING_REGEX, '[".*evilPlan.*"]')
    assert get_omitting_regex().pattern == "(.*evilPlan.*)"


def test_get_omitting_regexes_backward_compatibility(monkeypatch):
    monkeypatch.setenv(LUMIGO_SECRET_MASKING_REGEX_BACKWARD_COMP, '[".*evilPlan.*"]')
    assert get_omitting_regex().pattern == "(.*evilPlan.*)"


def test_get_omitting_regexes_prefer_new_environment_name(monkeypatch):
    monkeypatch.setenv(LUMIGO_SECRET_MASKING_REGEX, '[".*evilPlan.*"]')
    monkeypatch.setenv(LUMIGO_SECRET_MASKING_REGEX_BACKWARD_COMP, '[".*evilPlan2.*"]')
    assert get_omitting_regex().pattern == "(.*evilPlan.*)"


def test_get_omitting_regexes_fallback(monkeypatch):
    expected = "(.*pass.*|.*key.*|.*secret.*|.*credential.*|SessionToken|x-amz-security-token|Signature|Authorization)"
    assert get_omitting_regex().pattern == expected


def test_omit_keys_environment(monkeypatch):
    monkeypatch.setenv(LUMIGO_SECRET_MASKING_REGEX, '[".*evilPlan.*"]')
    value = {"password": "abc", "evilPlan": {"take": "over", "the": "world"}}
    assert omit_keys(value)[0] == {"password": "abc", "evilPlan": "****"}


@pytest.mark.parametrize("configuration_value", (True, False))
def test_config_enhanced_print_with_envs(monkeypatch, configuration_value):
    monkeypatch.setenv("LUMIGO_ENHANCED_PRINT", "TRUE")
    config(enhance_print=configuration_value)
    assert Configuration.enhanced_print is True


@pytest.mark.parametrize("configuration_value", (True, False))
def test_config_enhanced_print_without_envs(monkeypatch, configuration_value):
    monkeypatch.delenv("LUMIGO_ENHANCED_PRINT", raising=False)
    config(enhance_print=configuration_value)
    assert Configuration.enhanced_print == configuration_value


@pytest.mark.parametrize("configuration_value", (True, False))
def test_config_enhanced_printstep_function_with_envs(monkeypatch, configuration_value):
    monkeypatch.setenv("LUMIGO_STEP_FUNCTION", "TRUE")
    config(step_function=configuration_value)
    assert Configuration.is_step_function is True


@pytest.mark.parametrize("configuration_value", (True, False))
def test_config_enhanced_printstep_function_without_envs(monkeypatch, configuration_value):
    monkeypatch.delenv("LUMIGO_STEP_FUNCTION", raising=False)
    config(step_function=configuration_value)
    assert Configuration.is_step_function == configuration_value


def test_config_timeout_timer_buffer_with_exception(monkeypatch):
    monkeypatch.setenv("LUMIGO_TIMEOUT_BUFFER", "not float")
    config()
    assert Configuration.timeout_timer_buffer is None


def test_warn_client_print(capsys):
    warn_client("message")
    assert capsys.readouterr().out.startswith(f"{WARN_CLIENT_PREFIX}: message")


def test_warn_client_dont_print(capsys, monkeypatch):
    monkeypatch.setenv("LUMIGO_WARNINGS", "off")
    warn_client("message")
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    "remaining_time, conf, expected",
    ((3, 1, 1), (3, None, 0.5), (10, None, 1), (20, None, 2), (900, None, 3)),
)
def test_get_timeout_buffer(remaining_time, conf, expected):
    Configuration.timeout_timer_buffer = conf
    assert get_timeout_buffer(remaining_time) == expected


@pytest.mark.parametrize(
    ["arg", "host"],
    [("https://a.com", "a.com"), (f"https://b.com{EDGE_PATH}", "b.com"), ("h.com", "h.com")],
)
def test_prepare_host(arg, host):
    assert prepare_host(arg) == host


@pytest.mark.parametrize(
    "errors, final_log", [(ValueError, "ERROR"), ([ValueError, Mock()], "INFO")]
)
def test_report_json_retry(monkeypatch, reporter_mock, caplog, errors, final_log):
    reporter_mock.side_effect = report_json
    monkeypatch.setattr(Configuration, "host", "force_reconnect")
    monkeypatch.setattr(Configuration, "should_report", True)
    monkeypatch.setattr(http.client, "HTTPSConnection", Mock())
    http.client.HTTPSConnection("force_reconnect").getresponse.side_effect = errors

    report_json(None, [{"a": "b"}])

    assert caplog.records[-1].levelname == final_log
