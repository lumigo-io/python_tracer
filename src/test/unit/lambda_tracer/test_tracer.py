import datetime
import http.client
import json
import logging
import os
import re
import traceback
from decimal import Decimal
from functools import wraps
from unittest.mock import ANY, MagicMock

import boto3
import pytest
from lumigo_core.configuration import CoreConfiguration
from lumigo_core.scrubbing import EXECUTION_TAGS_KEY
from moto import mock_kinesis

from lumigo_tracer import LumigoChalice, add_execution_tag, lumigo_tracer
from lumigo_tracer.lambda_tracer import lambda_reporter
from lumigo_tracer.lambda_tracer.lambda_reporter import (
    _create_request_body,
    report_json,
)
from lumigo_tracer.lambda_tracer.spans_container import ENRICHMENT_TYPE, SpansContainer
from lumigo_tracer.lumigo_utils import (
    EDGE_KINESIS_STREAM_NAME,
    LUMIGO_EVENT_KEY,
    LUMIGO_PROPAGATE_W3C,
    SKIP_COLLECTING_HTTP_BODY_KEY,
    STEP_FUNCTION_UID_KEY,
)
from lumigo_tracer.w3c_context import TRACEPARENT_HEADER_NAME

TOKEN = "t_10faa5e13e7844aaa1234"


def test_lambda_wrapper_basic_events(reporter_mock, context):
    """
    This test checks that the basic events (start and end messages) has been sent.
    """

    @lumigo_tracer(token=TOKEN)
    def lambda_test_function(event, context):
        pass

    lambda_test_function({}, context)
    function_span = SpansContainer.get_span().function_span
    assert not SpansContainer.get_span().spans
    assert "started" in function_span
    assert "ended" in function_span
    assert reporter_mock.call_count == 2
    first_send = reporter_mock.call_args_list[0][1]["msgs"]
    assert len(first_send) == 1
    assert first_send[0]["id"].endswith("_started")
    assert first_send[0]["maxFinishTime"]


@pytest.mark.parametrize("token", ["t_", "", "123456789101112"])
def test_lambda_wrapper_validate_token_format_not_valid(context, capsys, token):
    """
    This test checks that the token has a valid format (sends warning since all inputs are invalid)
    """

    @lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        pass

    lambda_test_function({}, context)
    captured = capsys.readouterr()
    expected = "Lumigo Warning: Invalid Token. Go to Lumigo Settings to get a valid token.\n"
    assert captured[0] == expected


@pytest.mark.parametrize(
    "token",
    [
        "t_22819b63j7567h6",
        "t_22819b633fe97a4d0ed1",
        "t_22819b63j7567h65jy568j5hj6589y6y6j859j68h695h6j685986h66h6h",
    ],
)
def test_lambda_wrapper_validate_token_format_valid(context, capsys, token):
    """
    This test checks that the token has a valid format and no warning
    """

    @lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        pass

    lambda_test_function({}, context)
    captured = capsys.readouterr()
    expected = "Lumigo Warning: Invalid Token. Go to Lumigo Settings to get a valid token.\n"
    assert captured[0] != expected


@pytest.mark.parametrize("exc", [ValueError("Oh no"), ValueError(), ValueError(Exception())])
def test_lambda_wrapper_exception(exc, context):
    @lumigo_tracer(token=TOKEN)
    def lambda_test_function(event, context):
        a = "A"  # noqa
        raise exc

    try:
        lambda_test_function({}, context)
    except ValueError:
        pass
    else:
        assert False

    function_span = SpansContainer.get_span().function_span
    assert not SpansContainer.get_span().spans
    assert function_span.get("error", {}).get("type") == "ValueError"
    # Make sure no lumigo_tracer
    assert len(function_span["error"]["frames"]) == 1
    assert function_span["error"]["frames"][0].pop("lineno") > 0
    assert function_span["error"]["frames"][0] == {
        "function": "lambda_test_function",
        "fileName": __file__,
        "variables": {
            "a": '"A"',
            "context": f'"{str(context)}"',
            "event": "{}",
            "exc": f'"{str(exc)}"',
        },
    }
    assert not function_span["id"].endswith("_started")
    assert "reporter_rtt" in function_span
    assert "maxFinishTime" not in function_span
    # Test that we can create an output message out of this span
    assert _create_request_body(None, [function_span], prune_size_flag=False, should_try_zip=False)


def test_lambda_wrapper_return_decimal(context):
    @lumigo_tracer(token=TOKEN)
    def lambda_test_function(event, context):
        return {"a": [Decimal(1)]}

    lambda_test_function({}, context)
    span = SpansContainer.get_span().function_span
    assert span["return_value"] == '{"a": [1.0]}'


def test_lambda_wrapper_provision_concurrency_is_warm(context, monkeypatch):
    monkeypatch.setattr(SpansContainer, "is_cold", True)
    monkeypatch.setenv("AWS_LAMBDA_INITIALIZATION_TYPE", "provisioned-concurrency")

    @lumigo_tracer(token=TOKEN)
    def lambda_test_function(event, context):
        return {"a": "b"}

    lambda_test_function({}, context)
    span = SpansContainer.get_span().function_span
    assert span["readiness"] == "warm"


def test_kill_switch(monkeypatch, context):
    monkeypatch.setattr(os, "environ", {"LUMIGO_SWITCH_OFF": "true"})

    @lumigo_tracer(token=TOKEN)
    def lambda_test_function(event, context):
        return 1

    assert lambda_test_function({}, context) == 1
    assert not SpansContainer._span


def test_wrapping_exception(monkeypatch, context):
    monkeypatch.setattr(SpansContainer, "create_span", lambda *args, **kwargs: 1 / 0)

    @lumigo_tracer(token=TOKEN)
    def lambda_test_function(event, context):
        return 1

    assert lambda_test_function({}, context) == 1
    assert not SpansContainer._span


def test_wrapping_with_parameters(context):
    @lumigo_tracer(should_report="123")
    def lambda_test_function(event, context):
        return 1

    assert lambda_test_function({}, context) == 1
    assert CoreConfiguration.should_report == "123"


def test_wrapping_print_happy_flow(context, capsys):
    @lumigo_tracer()
    def lambda_test_function(event, context):
        print("hello")
        return 1

    assert lambda_test_function({}, context) == 1
    assert "hello" in capsys.readouterr().out


def test_wrapping_enhanced_print_backward_compatible(context, capsys):
    @lumigo_tracer(enhance_print=True)
    def lambda_test_function(event, context):
        print("hello")
        return 1

    assert lambda_test_function({}, context) == 1
    assert "hello" in capsys.readouterr().out


@pytest.mark.parametrize("is_verbose", [True, False])
def test_skip_collecting_http_parts(wrap_all_libraries, monkeypatch, context, is_verbose):
    if is_verbose:
        monkeypatch.setenv("LUMIGO_VERBOSE", "false")
    else:
        monkeypatch.setenv(SKIP_COLLECTING_HTTP_BODY_KEY, "true")

    @lumigo_tracer()
    def lambda_test_function(event, context):
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("POST", "/", json.dumps({"a": "b"}))
        return {"hello": "world"}

    lambda_test_function({}, context)
    http_spans = list(SpansContainer.get_span().spans.values())
    assert http_spans[0]["info"]["httpInfo"]["request"]["body"] == ""
    if is_verbose:
        assert "uri" not in http_spans[0]["info"]["httpInfo"]["request"]
        assert "headers" not in http_spans[0]["info"]["httpInfo"]["request"]
    else:
        assert http_spans[0]["info"]["httpInfo"]["request"]["uri"] == "www.google.com/"
        assert http_spans[0]["info"]["httpInfo"]["request"]["headers"]


@pytest.mark.parametrize("propagate_w3c", [True, False])
def test_add_w3c_headers_to_http_without_headers(
    wrap_all_libraries, monkeypatch, context, propagate_w3c, aws_env
):
    os.environ["LUMIGO_PROPAGATE_W3C"] = "TRUE" if propagate_w3c else "FALSE"

    @lumigo_tracer()
    def lambda_test_function(event, context):
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("POST", "/", json.dumps({"a": "b"}))
        return {"hello": "world"}

    lambda_test_function({}, context)
    http_spans = list(SpansContainer.get_span().spans.values())
    actual_headers = json.loads(http_spans[0]["info"]["httpInfo"]["request"]["headers"])
    assert (TRACEPARENT_HEADER_NAME in actual_headers) == propagate_w3c


def test_add_w3c_headers_to_http_with_headers_as_args(
    wrap_all_libraries, monkeypatch, context, aws_env
):
    monkeypatch.setenv(LUMIGO_PROPAGATE_W3C, "TRUE")

    @lumigo_tracer()
    def lambda_test_function(event, context):
        conn = http.client.HTTPConnection("www.google.com")
        conn.request(
            "POST", "/", json.dumps({"a": "b"}), {"another": "header"}, encode_chunked=True
        )
        return {"hello": "world"}

    lambda_test_function({}, context)
    http_spans = list(SpansContainer.get_span().spans.values())
    actual_headers = json.loads(http_spans[0]["info"]["httpInfo"]["request"]["headers"])
    assert actual_headers[TRACEPARENT_HEADER_NAME]
    assert actual_headers["another"] == "header"


def test_add_w3c_headers_to_http_with_headers_as_kwargs(
    wrap_all_libraries, monkeypatch, context, aws_env
):
    monkeypatch.setenv(LUMIGO_PROPAGATE_W3C, "TRUE")

    @lumigo_tracer()
    def lambda_test_function(event, context):
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("POST", "/", json.dumps({"a": "b"}), headers={"another": "header"})
        return {"hello": "world"}

    lambda_test_function({}, context)
    http_spans = list(SpansContainer.get_span().spans.values())
    actual_headers = json.loads(http_spans[0]["info"]["httpInfo"]["request"]["headers"])
    assert actual_headers[TRACEPARENT_HEADER_NAME]
    assert actual_headers["another"] == "header"


def test_lumigo_chalice(context, monkeypatch):
    # mimic aws env
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_VERSION", "true")

    class App:
        @property
        def a(self):
            return "a"

        def b(self):
            return "b"

        def __call__(self, *args, **kwargs):
            return "c"

    app = App()
    app = LumigoChalice(app)

    # should not use lumigo's wrapper
    assert app.a == "a"
    assert app.b() == "b"
    assert not SpansContainer._span

    # should create a new span (but return the original value)
    assert app({}, context) == "c"
    assert SpansContainer._span


def test_lumigo_chalice_create_extra_lambdas(monkeypatch, context):
    # mimic aws env
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_VERSION", "true")

    class Chalice:
        """
        This class in a mimic of chalice.
        """

        touched = False

        @staticmethod
        def on_s3_event(**kwargs):
            Chalice.touched = True  # represents chalice's global analysis (in the deploy process)

            def _create_registration_function(func):
                @wraps(func)
                def user_lambda_handler(*args, **kwargs):
                    return func(*args, **kwargs)

                return user_lambda_handler

            return _create_registration_function

    app = Chalice()
    app = LumigoChalice(app)

    @app.on_s3_event(name="test")
    def handler(event, context):
        return "hello world"

    # should run the outer code before lambda execution, but not create span (in global execution)
    assert app.touched
    assert not SpansContainer._span

    # should create a new span (but return the original value)
    assert handler({}, context) == "hello world"
    assert SpansContainer._span


def test_lumigo_chalice_disabled_when_not_in_aws(monkeypatch):
    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_VERSION", raising=False)
    monkeypatch.delenv("LUMIGO_SWITCH_OFF", raising=False)
    assert LumigoChalice("myApp") == "myApp"


def test_lumigo_chalice_disabled_when_switch_off(monkeypatch):
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_VERSION", "true")
    monkeypatch.setenv("LUMIGO_SWITCH_OFF", "true")
    assert LumigoChalice("myApp") == "myApp"


@pytest.mark.parametrize(
    "event, expected_trigger",
    [
        ({}, []),
        (
            {"result": 1, LUMIGO_EVENT_KEY: {STEP_FUNCTION_UID_KEY: "123"}},
            [{"fromMessageIds": ["123"], "targetId": None, "triggeredBy": "stepFunction"}],
        ),
    ],
)
def test_wrapping_step_function(event, expected_trigger, context):
    @lumigo_tracer(step_function=True)
    def lambda_test_function(event, context):
        return {"result": 1}

    lambda_test_function(event, context)
    span = SpansContainer.get_span()
    assert len(span.spans) == 1
    trigger = span.function_span["info"].get("trigger", [])
    [t.pop("id") for t in trigger]
    assert trigger == expected_trigger
    return_value = json.loads(span.function_span["return_value"])
    assert return_value["result"] == 1
    assert return_value[LUMIGO_EVENT_KEY][STEP_FUNCTION_UID_KEY]
    assert list(span.spans.values())[0]["info"]["httpInfo"]["host"] == "StepFunction"


def test_omitting_keys(wrap_all_libraries, context):
    @lumigo_tracer()
    def lambda_test_function(event, context):
        d = {"a": "b", "myPassword": "123"}
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("POST", "/", json.dumps(d))
        return {"secret_password": "lumigo rulz"}  # pragma: allowlist secret

    lambda_test_function({"key": "24"}, context)
    span = SpansContainer.get_span()
    assert span.function_span["return_value"] == '{"secret_password": "****"}'
    assert span.function_span["event"] == '{"key": "****"}'
    http_spans = list(SpansContainer.get_span().spans.values())
    spans = json.loads(_create_request_body(None, http_spans, True, False))
    assert spans[0]["info"]["httpInfo"]["request"]["body"] == json.dumps(
        {"a": "b", "myPassword": "****"}
    )


def test_can_not_wrap_twice(reporter_mock, context):
    @lumigo_tracer()
    @lumigo_tracer()
    def lambda_test_function(event, context):
        return "ret_value"

    result = lambda_test_function({}, context)
    assert result == "ret_value"
    assert reporter_mock.call_count == 2


def get_enrichment_spans(reporter_mock):
    final_send = reporter_mock.call_args_list[-1][1]["msgs"]
    return [s for s in final_send if s["type"] == ENRICHMENT_TYPE]


def test_wrapping_with_tags(context, reporter_mock, lambda_traced):
    key = "my_key"
    value = "my_value"

    @lumigo_tracer()
    def lambda_test_function(event, context):
        add_execution_tag(key, value)
        return "ret_value"

    result = lambda_test_function({}, context)
    assert result == "ret_value"
    enrichment_spans = get_enrichment_spans(reporter_mock)
    assert len(enrichment_spans) == 1
    assert enrichment_spans[0][EXECUTION_TAGS_KEY] == [{"key": key, "value": value}]


@pytest.mark.parametrize(
    "key, event",
    [("my_key", {"my_key": "my_value"}), ("my_key.key2", {"my_key": {"key2": "my_value"}})],
)
def test_wrapping_with_auto_tags(context, key, event, reporter_mock, lambda_traced):
    @lumigo_tracer(auto_tag=[key])
    def lambda_test_function(event, context):
        return "ret_value"

    result = lambda_test_function(event, context)
    assert result == "ret_value"
    enrichment_spans = get_enrichment_spans(reporter_mock)
    assert len(enrichment_spans) == 1
    assert enrichment_spans[0][EXECUTION_TAGS_KEY] == [{"key": key, "value": "my_value"}]


def test_not_jsonable_return_value_python37(monkeypatch, context):
    monkeypatch.setenv("AWS_EXECUTION_ENV", "AWS_Lambda_python3.7")

    @lumigo_tracer()
    def lambda_test_function(event, context):
        return {"a": datetime.datetime.now()}

    lambda_test_function({}, context)

    function_span = SpansContainer.get_span().function_span
    assert function_span["return_value"] is None
    assert function_span["error"]["type"] == "ReturnValueError"
    # following python's runtime: runtime/lambda_runtime_marshaller.py:27
    expected_message = 'The lambda will probably fail due to bad return value. Original message: "Object of type datetime is not JSON serializable"'
    assert function_span["error"]["message"] == expected_message


def test_not_jsonable_return_value_non_python37(monkeypatch, context, caplog):
    monkeypatch.setenv("AWS_EXECUTION_ENV", "AWS_Lambda_python3.8")

    @lumigo_tracer()
    def lambda_test_function(event, context):
        return {"a": datetime.datetime.now()}

    lambda_test_function({}, context)

    function_span = SpansContainer.get_span().function_span
    assert function_span["return_value"] is None
    assert "error" not in function_span
    assert next(
        log
        for log in caplog.records
        if log.levelno == logging.ERROR
        and "Could not serialize the return value of the lambda" in log.message
    )


@mock_kinesis
def test_china(context, reporter_mock, monkeypatch):
    china_region_for_test = "ap-east-1"  # Moto doesn't work for China
    monkeypatch.setattr(lambda_reporter, "CHINA_REGION", china_region_for_test)
    monkeypatch.setenv("AWS_REGION", china_region_for_test)
    reporter_mock.side_effect = report_json  # Override the conftest's monkeypatch
    access_key_id = "my_access_key_id"
    secret_access_key = "my_secret_access_key"  # pragma: allowlist secret
    # Create edge Kinesis
    client = boto3.client(
        "kinesis",
        region_name=china_region_for_test,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
    )
    client.create_stream(StreamName=EDGE_KINESIS_STREAM_NAME, ShardCount=1)
    shard_id = client.describe_stream(StreamName=EDGE_KINESIS_STREAM_NAME)["StreamDescription"][
        "Shards"
    ][0]["ShardId"]
    shard_iterator = client.get_shard_iterator(
        StreamName=EDGE_KINESIS_STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType="AT_TIMESTAMP",
        Timestamp=datetime.datetime.utcnow(),
    )["ShardIterator"]

    original_get_boto_client = boto3.client
    monkeypatch.setattr(boto3, "client", MagicMock(side_effect=original_get_boto_client))

    @lumigo_tracer(
        edge_kinesis_aws_access_key_id=access_key_id,
        edge_kinesis_aws_secret_access_key=secret_access_key,
        should_report=True,
    )
    def lambda_test_function(event, context):
        return "ret_value"

    event = {"k": "v"}
    result = lambda_test_function(event, context)

    assert result == "ret_value"
    # Spans sent to Kinesis
    records = client.get_records(ShardIterator=shard_iterator)["Records"]
    assert len(records) == 2  # Start span and end span
    span_sent = json.loads(records[1]["Data"].decode())[0]
    assert span_sent["event"] == json.dumps(event)
    # Used the client from the decorator params
    boto3.client.assert_called_with(
        "kinesis",
        region_name=china_region_for_test,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        config=ANY,
    )


def test_lumigo_tracer_doesnt_change_exception(context):
    @lumigo_tracer(token=TOKEN)
    def wrapped(event, context):
        raise Exception("Inner exception")

    with pytest.raises(Exception):
        wrapped({}, context)

    def wrapped(event, context):
        raise Exception("Inner exception")

    with pytest.raises(Exception) as e:
        wrapped({}, context)

    stacktrace = SpansContainer.get_span().function_span["error"]["stacktrace"]
    assert "lumigo_tracer/lambda_tracer/tracer.py" not in stacktrace
    line_dropper = re.compile(r"\d{3}")
    from_lumigo = line_dropper.sub("-", stacktrace)
    original = line_dropper.sub("-", traceback.format_tb(e.value.__traceback__)[1])
    assert from_lumigo == original


def test_cold_indicator_with_request_in_cold_phase(context):
    SpansContainer.is_cold = True
    #  Create a request the might invert the `is_cold` field
    http.client.HTTPConnection("www.google.com").request("POST", "/")
    assert SpansContainer.is_cold is True

    @lumigo_tracer(step_function=True)
    def lambda_test_function(event, context):
        http.client.HTTPConnection("www.google.com").request("POST", "/")

    lambda_test_function({}, context)
    assert SpansContainer.get_span().function_span["readiness"] == "cold"
    assert SpansContainer.is_cold is False
