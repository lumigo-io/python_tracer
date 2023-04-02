import datetime
import inspect
import logging
from collections import OrderedDict
from decimal import Decimal

import pytest

from lumigo_tracer import lumigo_utils
from lumigo_tracer.lumigo_utils import (
    DEFAULT_AUTO_TAG_KEY,
    INTERNAL_ANALYTICS_PREFIX,
    KILL_SWITCH,
    LUMIGO_SECRET_MASKING_REGEX,
    LUMIGO_SECRET_MASKING_REGEX_BACKWARD_COMP,
    MAX_VAR_LEN,
    MAX_VARS_SIZE,
    SKIP_SCRUBBING_KEYS,
    TRUNCATE_SUFFIX,
    WARN_CLIENT_PREFIX,
    Configuration,
    _truncate_locals,
    concat_old_body_to_new,
    config,
    format_frame,
    format_frames,
    get_omitting_regex,
    get_size_upper_bound,
    get_timeout_buffer,
    internal_analytics_message,
    is_aws_arn,
    is_error_code,
    is_kill_switch_on,
    is_python_37,
    is_span_has_error,
    lumigo_dumps,
    lumigo_safe_execute,
    omit_keys,
    warn_client,
)


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
    assert is_span_has_error(input_span) is expected_is_error


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
        (  # Dict of long list
            {"a": [{"key": "value", "password": "value", "b": "c"}]},
            '{"a": [{"key": "****", "password": "****", "b": "c"}]}',
        ),
        (  # Multiple nested lists
            {"a": [[[{"c": [{"key": "v"}]}], [{"c": [{"key": "v"}]}]]]},
            '{"a": [[[{"c": [{"key": "****"}]}], [{"c": [{"key": "****"}]}]]]}',
        ),
        (  # non jsonable
            {"set"},
            "{'set'}",
        ),
        (  # redump already dumped and truncated json (avoid re-escaping)
            f'{{"a": "b{TRUNCATE_SUFFIX}',
            f'{{"a": "b{TRUNCATE_SUFFIX}',
        ),
    ],
)
def test_lumigo_dumps(value, output):
    assert lumigo_dumps(value, max_size=100) == output


def test_lumigo_dumps_fails_on_non_jsonable():
    with pytest.raises(TypeError):
        lumigo_utils({"set"})


@pytest.mark.parametrize(
    ("value", "omit_skip_path", "output"),
    [
        ({"a": "b", "Key": "v"}, ["Key"], '{"a": "b", "Key": "v"}'),  # Not nested
        (  # Nested with list
            {"R": [{"o": {"key": "value"}}]},
            ["R", "o", "key"],
            '{"R": [{"o": {"key": "value"}}]}',
        ),
        (  # Doesnt affect other paths
            {"a": {"key": "v"}, "b": {"key": "v"}},
            ["a", "key"],
            '{"a": {"key": "v"}, "b": {"key": "****"}}',
        ),
        (  # Nested items not affected
            {"key": {"password": "v"}},
            ["key"],
            '{"key": {"password": "****"}}',
        ),
        (  # Happy flow - nested case
            {"key": {"password": "v"}},
            ["key", "password"],
            '{"key": {"password": "v"}}',
        ),
        ({"a": {"key": "c"}}, ["key"], '{"a": {"key": "****"}}'),  # Affect only the full path
    ],
)
def test_lumigo_dumps_with_omit_skip(value, omit_skip_path, output):
    assert lumigo_dumps(value, omit_skip_path=omit_skip_path) == output


def test_lumigo_dumps_with_omit_skip_and_should_scrub_known_services(monkeypatch):
    monkeypatch.setenv("LUMIGO_SCRUB_KNOWN_SERVICES", "true")
    config()

    assert lumigo_dumps({"key": "v"}, omit_skip_path=["key"]) == '{"key": "****"}'


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
def test_config_step_function_with_envs(monkeypatch, configuration_value):
    monkeypatch.setenv("LUMIGO_STEP_FUNCTION", "TRUE")
    config(step_function=configuration_value)
    assert Configuration.is_step_function is True


@pytest.mark.parametrize("configuration_value", (True, False))
def test_config_step_function_without_envs(monkeypatch, configuration_value):
    monkeypatch.delenv("LUMIGO_STEP_FUNCTION", raising=False)
    config(step_function=configuration_value)
    assert Configuration.is_step_function == configuration_value


@pytest.mark.parametrize("value, expected", (("TRUE", True), ("FALSE", False)))
def test_config_propagate_w3c_by_env(monkeypatch, value, expected):
    monkeypatch.setenv("LUMIGO_PROPAGATE_W3C", value)
    config()
    assert Configuration.propagate_w3c == expected


def test_config_propagate_w3c_default_value(monkeypatch):
    monkeypatch.delenv("LUMIGO_PROPAGATE_W3C", raising=False)
    config()
    assert Configuration.propagate_w3c is False


def test_config_lumigo_auto_tag(monkeypatch):
    monkeypatch.setenv("LUMIGO_AUTO_TAG", "key1,key2")
    config()
    assert Configuration.auto_tag == ["key1", "key2"]


def test_config_lumigo_no_auto_tag_env(monkeypatch):
    monkeypatch.delenv("LUMIGO_AUTO_TAG", raising=False)
    config()
    assert Configuration.auto_tag == [DEFAULT_AUTO_TAG_KEY]


def test_config_lumigo_auto_tag_kwarg(monkeypatch):
    monkeypatch.delenv("LUMIGO_AUTO_TAG", raising=False)
    config(auto_tag=["key1", "key2"])
    assert Configuration.auto_tag == ["key1", "key2"]


def test_config_lumigo_domains_scrubber_with_envs(monkeypatch):
    monkeypatch.setenv("LUMIGO_DOMAINS_SCRUBBER", '["lambda.us-west-2.amazonaws.com"]')
    config()
    assert Configuration.domains_scrubber.pattern == "(lambda.us-west-2.amazonaws.com)"


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


@pytest.mark.parametrize("env, expected", [("True", True), ("other", False), ("123", False)])
def test_is_kill_switch_on(monkeypatch, env, expected):
    monkeypatch.setenv(KILL_SWITCH, env)
    assert is_kill_switch_on() == expected


def test_get_max_entry_size_default(monkeypatch):
    assert Configuration.get_max_entry_size() == 2048


def test_get_max_entry_size_has_error():
    assert Configuration.get_max_entry_size(has_error=True) == 4096


def test_get_size_upper_bound():
    assert get_size_upper_bound() == 4096


@pytest.mark.parametrize(
    ("status_code", "is_error"), [(0, False), (200, False), (400, True), (500, True)]
)
def test_is_error_code(status_code, is_error):
    assert is_error_code(status_code) is is_error


@pytest.mark.parametrize(
    ("arn", "is_arn_result"),
    [
        ("not-arn", False),
        (None, False),
        ("arn:aws:lambda:region:876841109798:function:function-name", True),
    ],
)
def test_is_aws_arn(arn, is_arn_result):
    assert is_aws_arn(arn) is is_arn_result


def test_internal_analytics_message(capsys):
    internal_analytics_message("Message", force=True)
    assert capsys.readouterr().out.startswith(INTERNAL_ANALYTICS_PREFIX)
    internal_analytics_message("Message")
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    "old, new, expected",
    [
        (b"1", b"2", b"12"),  # happy flow
        (b"", b"2", b"2"),  # no old
        (b"1", b"", b"1"),  # no new
        (b"12", b"34567", b'"1234...[too long]'),  # together pass max size
        (b"123456", b"789", b'"1234...[too long]'),  # old pass max size
        (b'{"a": "b"}', b"", b'{"a": "b"}'),  # json
        (b'a"b', b"c", b'a"bc'),  # data with "
        (b'{"a": "\\""}', b"", b'{"a": "\\""}'),  # json with "
        (b"\xa0", b"\xa1", b"\xa0\xa1"),  # non decode-able string
    ],
)
def test_concat_old_body_to_new(old, new, expected, monkeypatch):
    monkeypatch.setattr(Configuration, "max_entry_size", 5)
    assert concat_old_body_to_new(lumigo_dumps(old), new) == lumigo_dumps(expected)


@pytest.mark.parametrize("severity", [logging.DEBUG, logging.ERROR])
def test_lumigo_safe_execute_with_level(severity, caplog):
    with lumigo_safe_execute("test", severity=severity):
        raise ValueError("Failing")
    assert caplog.records[-1].levelno == severity


@pytest.mark.parametrize(
    "env_value, expected",
    [
        ("AWS_Lambda_python3.8", False),
        ("AWS_Lambda_python3.7", True),
    ],
)
def test_is_python_37(monkeypatch, env_value, expected):
    monkeypatch.setenv("AWS_EXECUTION_ENV", env_value)
    assert is_python_37() == expected


def test_is_python_37_without_env(monkeypatch):
    monkeypatch.delenv("AWS_EXECUTION_ENV", raising=False)
    assert is_python_37() is False
