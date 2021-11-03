import importlib.util
import inspect
import hashlib
import logging
import os
import uuid
from collections import OrderedDict
from decimal import Decimal
import datetime
import http.client
import socket
from unittest.mock import Mock

import boto3
from mock import Mock, MagicMock

import pytest
from lumigo_tracer import lumigo_utils
from lumigo_tracer.lumigo_utils import (
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
    get_edge_host,
    EDGE_PATH,
    report_json,
    is_kill_switch_on,
    KILL_SWITCH,
    is_error_code,
    get_size_upper_bound,
    is_aws_arn,
    CHINA_REGION,
    internal_analytics_message,
    INTERNAL_ANALYTICS_PREFIX,
    InternalState,
    aws_dump,
    concat_old_body_to_new,
    TRUNCATE_SUFFIX,
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


def test_config_lumigo_domains_scrubber_with_envs(monkeypatch):
    monkeypatch.setenv("LUMIGO_DOMAINS_SCRUBBER", '["lambda.us-west-2.amazonaws.com"]')
    config()
    assert len(Configuration.domains_scrubber) == 1


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
def test_get_edge_host(arg, host, monkeypatch):
    monkeypatch.setattr(Configuration, "host", arg)
    assert get_edge_host("region") == host


def test_report_json_extension_spans_mode(monkeypatch, reporter_mock, tmpdir):
    extension_dor = tmpdir.mkdir("tmp")
    monkeypatch.setattr(lumigo_utils, "get_extension_dir", lambda *args, **kwargs: extension_dor)
    monkeypatch.setattr(uuid, "uuid4", lambda *args, **kwargs: "span_name")
    monkeypatch.setattr(Configuration, "should_report", True)
    monkeypatch.setenv("LUMIGO_USE_TRACER_EXTENSION", "TRUE")
    mocked_urandom = MagicMock(hex=MagicMock(return_value="my_mocked_data"))
    monkeypatch.setattr(os, "urandom", lambda *args, **kwargs: mocked_urandom)

    start_span = [{"span": "true"}]
    report_json(region=None, msgs=start_span, is_start_span=True)

    spans = []
    size_factor = 100
    for i in range(size_factor):
        spans.append(
            {
                i: "a" * size_factor,
            }
        )
    report_json(region=None, msgs=spans, is_start_span=False)
    start_path_path = f"{lumigo_utils.get_extension_dir()}/span_name_span"
    end_path_path = f"{lumigo_utils.get_extension_dir()}/span_name_end"
    start_file_content = json.loads(open(start_path_path, "r").read())
    end_file_content = json.loads(open(end_path_path, "r").read())
    assert start_span == start_file_content
    assert json.dumps(end_file_content) == json.dumps(spans)



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


def test_report_json_fast_failure_after_timeout(monkeypatch, reporter_mock, caplog):
    reporter_mock.side_effect = report_json
    monkeypatch.setattr(Configuration, "host", "host")
    monkeypatch.setattr(Configuration, "should_report", True)
    monkeypatch.setattr(http.client, "HTTPSConnection", Mock())
    http.client.HTTPSConnection("force_reconnect").getresponse.side_effect = socket.timeout

    assert report_json(None, [{"a": "b"}]) == 0
    assert caplog.records[-1].msg == "Timeout while connecting to host"

    assert report_json(None, [{"a": "b"}]) == 0
    assert caplog.records[-1].msg == "Skip sending messages due to previous timeout"

    InternalState.timeout_on_connection = datetime.datetime(2016, 1, 1)
    assert report_json(None, [{"a": "b"}]) == 0
    assert caplog.records[-1].msg == "Timeout while connecting to host"


def test_report_json_china_missing_access_key_id(monkeypatch, reporter_mock, caplog):
    monkeypatch.setattr(Configuration, "should_report", True)
    reporter_mock.side_effect = report_json
    assert report_json(CHINA_REGION, [{"a": "b"}]) == 0
    assert any(
        "edge_kinesis_aws_access_key_id" in record.message and record.levelname == "ERROR"
        for record in caplog.records
    )


def test_report_json_china_missing_secret_access_key(monkeypatch, reporter_mock, caplog):
    monkeypatch.setattr(Configuration, "should_report", True)
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_access_key_id", "my_value")
    reporter_mock.side_effect = report_json
    assert report_json(CHINA_REGION, [{"a": "b"}]) == 0
    assert any(
        "edge_kinesis_aws_secret_access_key" in record.message and record.levelname == "ERROR"
        for record in caplog.records
    )


def test_report_json_china_no_boto(monkeypatch, reporter_mock, caplog):
    reporter_mock.side_effect = report_json
    monkeypatch.setattr(Configuration, "should_report", True)
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_access_key_id", "my_value")
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_secret_access_key", "my_value")
    monkeypatch.setattr(lumigo_utils, "boto3", None)

    report_json(CHINA_REGION, [{"a": "b"}])

    assert any(
        "boto3 is missing. Unable to send to Kinesis" in record.message
        and record.levelname == "ERROR"  # noqa
        for record in caplog.records
    )


def test_report_json_china_on_error_no_exception_and_notify_user(capsys, monkeypatch):
    monkeypatch.setattr(Configuration, "should_report", True)
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_access_key_id", "my_value")
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_secret_access_key", "my_value")
    monkeypatch.setattr(boto3, "client", MagicMock(side_effect=Exception))
    lumigo_utils.get_logger().setLevel(logging.CRITICAL)

    report_json(CHINA_REGION, [{"a": "b"}])

    assert "Failed to send spans" in capsys.readouterr().out


def test_china_shouldnt_establish_http_connection(monkeypatch):
    monkeypatch.setenv("AWS_REGION", CHINA_REGION)
    # Reload a duplicate of lumigo_utils
    spec = importlib.util.find_spec("lumigo_tracer.lumigo_utils")
    lumigo_utils_reloaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lumigo_utils_reloaded)

    assert lumigo_utils_reloaded.edge_connection is None


def test_china_with_env_variable_shouldnt_reuse_boto3_connection(monkeypatch):
    monkeypatch.setenv("LUMIGO_KINESIS_SHOULD_REUSE_CONNECTION", "false")
    monkeypatch.setattr(Configuration, "should_report", True)
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_access_key_id", "my_value")
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_secret_access_key", "my_value")
    monkeypatch.setattr(boto3, "client", MagicMock())

    report_json(CHINA_REGION, [{"a": "b"}])
    report_json(CHINA_REGION, [{"a": "b"}])

    assert boto3.client.call_count == 2


def test_china_reuse_boto3_connection(monkeypatch):
    monkeypatch.setattr(Configuration, "should_report", True)
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_access_key_id", "my_value")
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_secret_access_key", "my_value")
    monkeypatch.setattr(boto3, "client", MagicMock())

    report_json(CHINA_REGION, [{"a": "b"}])
    report_json(CHINA_REGION, [{"a": "b"}])

    boto3.client.assert_called_once()


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
