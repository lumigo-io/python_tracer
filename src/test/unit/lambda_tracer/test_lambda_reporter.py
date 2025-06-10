import gzip
import http.client
import importlib.util
import json
import logging
import os
import socket
import uuid
from base64 import b64decode
from datetime import datetime, timedelta
from unittest.mock import Mock

import boto3
import pytest
from lumigo_core.configuration import CoreConfiguration
from lumigo_core.scrubbing import EXECUTION_TAGS_KEY
from mock import MagicMock

from lumigo_tracer import lumigo_utils
from lumigo_tracer.lambda_tracer import lambda_reporter
from lumigo_tracer.lambda_tracer.lambda_reporter import (
    CHINA_REGION,
    EDGE_PATH,
    ENRICHMENT_TYPE,
    FUNCTION_TYPE,
    HTTP_TYPE,
    MONGO_SPAN,
    SPANS_SEND_SIZE_ENRICHMENT_SPAN_BUFFER,
    _create_request_body,
    _split_and_zip_spans,
    _update_enrichment_span_about_prioritized_spans,
    establish_connection,
    get_edge_host,
    get_event_base64_size,
    get_extension_dir,
    report_json,
)
from lumigo_tracer.lambda_tracer.spans_container import TOTAL_SPANS_KEY
from lumigo_tracer.lumigo_utils import Configuration, InternalState

NOW = datetime.now()
STARTED = (NOW - timedelta(seconds=10)).timestamp() * 1000
ENDED = NOW.timestamp() * 1000
DUMMY_SPAN = {"dummy": "dummy", "type": HTTP_TYPE}
FUNCTION_END_SPAN = {
    "dummy_end": "dummy_end",
    "type": FUNCTION_TYPE,
    "envs": {"var_name": "very_long_env_var_value"},
}
FUNCTION_END_SPAN_METADATA = {"dummy_end": "dummy_end", "type": FUNCTION_TYPE, "isMetadata": True}
ENRICHMENT_SPAN = {
    "type": ENRICHMENT_TYPE,
    "token": "token",
    "invocation_id": "request_id",
    "transaction_id": "transaction_id",
    "sending_time": "",
    EXECUTION_TAGS_KEY: [
        {"key": "exec_tag1", "value": "value1"},
        {"key": "exec_tag2", "value": "value2"},
        {"key": "exec_tag3", "value": "value3"},
        {"key": "exec_tag4", "value": "value4"},
        {"key": "exec_tag5", "value": "value5"},
        {"key": "exec_tag6", "value": "value6"},
        {"key": "exec_tag7", "value": "value7"},
        {"key": "exec_tag8", "value": "value8"},
        {"key": "exec_tag9", "value": "value9"},
        {"key": "exec_tag10", "value": "value10"},
    ],
    TOTAL_SPANS_KEY: 2,
}
ENRICHMENT_SPAN_METADATA = {
    "type": ENRICHMENT_TYPE,
    "token": "token",
    "invocation_id": "request_id",
    "transaction_id": "transaction_id",
    "sending_time": "",
    TOTAL_SPANS_KEY: 2,
    "isMetadata": True,
}
HTTP_SPAN = {
    "transactionId": "transaction-id",
    "id": "8b32c4b4-e483-4741-9eef-b8f8f6c72f66",
    "started": STARTED,
    "info": {
        "tracer": {"version": "1.1.230"},
        "traceId": {"Root": ""},
        "httpInfo": {
            "host": "www.google.com",
            "request": {
                "headers": '{"host": "www.google.com", "accept-encoding": "identity", "content-length": "0"}',
                "body": "very interesting body with very long text that should be filtered",
                "method": "POST",
                "uri": "www.google.com/",
                "instance_id": 4380969952,
            },
        },
    },
    "type": HTTP_TYPE,
    "account": "account-id",
    "region": "UNKNOWN",
    "parentId": "1234",
    "lambda_container_id": "4062b9eb-5f2d-4dde-9983-3f4404f30b5a",
    "token": "t_10faa5e13e7844aaa1234",
    "ended": ENDED,
}
HTTP_SPAN_METADATA = {
    "transactionId": "transaction-id",
    "id": "8b32c4b4-e483-4741-9eef-b8f8f6c72f66",
    "started": STARTED,
    "info": {
        "tracer": {"version": "1.1.230"},
        "traceId": {"Root": ""},
        "httpInfo": {
            "host": "www.google.com",
            "request": {"method": "POST", "uri": "www.google.com/", "instance_id": 4380969952},
        },
    },
    "type": HTTP_TYPE,
    "account": "account-id",
    "region": "UNKNOWN",
    "parentId": "1234",
    "lambda_container_id": "4062b9eb-5f2d-4dde-9983-3f4404f30b5a",
    "token": "t_10faa5e13e7844aaa1234",
    "ended": ENDED,
    "isMetadata": True,
}
ERROR_HTTP_SPAN = {
    "transactionId": "transaction-id",
    "id": "8b32c4b4-e483-4741-9eef-b8f8f6c72f66",
    "started": STARTED,
    "info": {
        "tracer": {"version": "1.1.230"},
        "traceId": {"Root": ""},
        "httpInfo": {
            "host": "www.google.com",
            "request": {
                "headers": '{"host": "www.google.com", "accept-encoding": "identity", "content-length": "0"}',
                "body": "very interesting body with very long text that should be filtered",
                "method": "POST",
                "uri": "www.google.com/",
                "instance_id": 4380969952,
            },
        },
    },
    "type": HTTP_TYPE,
    "account": "account-id",
    "region": "UNKNOWN",
    "parentId": "1234",
    "lambda_container_id": "4062b9eb-5f2d-4dde-9983-3f4404f30b5a",
    "token": "t_10faa5e13e7844aaa1234",
    "error": "ERROR",
    "ended": ENDED,
}
ERROR_HTTP_SPAN_METADATA = {
    "transactionId": "transaction-id",
    "id": "8b32c4b4-e483-4741-9eef-b8f8f6c72f66",
    "started": STARTED,
    "info": {
        "tracer": {"version": "1.1.230"},
        "traceId": {"Root": ""},
        "httpInfo": {
            "host": "www.google.com",
            "request": {"method": "POST", "uri": "www.google.com/", "instance_id": 4380969952},
        },
    },
    "type": HTTP_TYPE,
    "account": "account-id",
    "region": "UNKNOWN",
    "parentId": "1234",
    "lambda_container_id": "4062b9eb-5f2d-4dde-9983-3f4404f30b5a",
    "token": "t_10faa5e13e7844aaa1234",
    "error": "ERROR",
    "ended": ENDED,
    "isMetadata": True,
}
REDIS_SPAN = {
    "type": "redis",
    "id": "77d0b751-9496-4257-b910-25e33c029365",
    "connectionOptions": {"host": "lumigo", "port": None},
    "transactionId": "transaction-id",
    "started": STARTED,
    "requestArgs": '[[{"a": 1}], ["a"]]',
    "lambda_container_id": "b7bfabd6-bc95-445a-965d-513922afbcdd",
    "account": "account-id",
    "region": "UNKNOWN",
    "parentId": "parent-id",
    "info": {"tracer": {"version": "1.1.230"}, "traceId": {"Root": ""}},
    "requestCommand": '["SET", "GET"]',
    "token": "t_token",
    "ended": ENDED,
    "response": '"Very long result that we should cut because it is too long"',
}
REDIS_SPAN_METADATA = {
    "type": "redis",
    "id": "77d0b751-9496-4257-b910-25e33c029365",
    "connectionOptions": {"host": "lumigo", "port": None},
    "transactionId": "transaction-id",
    "started": STARTED,
    "lambda_container_id": "b7bfabd6-bc95-445a-965d-513922afbcdd",
    "account": "account-id",
    "region": "UNKNOWN",
    "parentId": "parent-id",
    "info": {"tracer": {"version": "1.1.230"}, "traceId": {"Root": ""}},
    "requestCommand": '["SET", "GET"]',
    "token": "t_token",
    "ended": ENDED,
    "isMetadata": True,
}
PYMONGO_SPAN = {
    "id": "6c86ff87-07b4-4663-9e2b-15acb75a81f0",
    "mongoOperationId": "oid",
    "type": MONGO_SPAN,
    "mongoConnectionId": "cid",
    "databaseName": "dname",
    "mongoRequestId": "rid",
    "lambda_container_id": "7fa30d38-3aed-4e15-96a7-b53116e2b5fa",
    "account": "account",
    "request": '"cmd"',
    "started": STARTED,
    "transactionId": "transaction-id",
    "region": "UNKNOWN",
    "parentId": "parent-id",
    "info": {"tracer": {"version": "1.1.230"}, "traceId": {"Root": ""}},
    "token": "token",
    "commandName": "cname",
    "ended": ENDED,
    "response": '{"code": 200}',
}
PYMONGO_SPAN_METADATA = {
    "id": "6c86ff87-07b4-4663-9e2b-15acb75a81f0",
    "mongoOperationId": "oid",
    "type": MONGO_SPAN,
    "mongoConnectionId": "cid",
    "databaseName": "dname",
    "mongoRequestId": "rid",
    "lambda_container_id": "7fa30d38-3aed-4e15-96a7-b53116e2b5fa",
    "account": "account",
    "started": STARTED,
    "transactionId": "transaction-id",
    "region": "UNKNOWN",
    "parentId": "parent-id",
    "info": {"tracer": {"version": "1.1.230"}, "traceId": {"Root": ""}},
    "token": "token",
    "commandName": "cname",
    "ended": ENDED,
    "isMetadata": True,
}
SQL_SPAN = {
    "parentId": "1234",
    "transactionId": "",
    "values": '["saart"]',
    "region": "UNKNOWN",
    "account": "",
    "query": '"INSERT INTO users (name) VALUES (?)"',
    "info": {"tracer": {"version": "1.1.230"}, "traceId": {"Root": ""}},
    "token": "t_10faa5e13e7844aaa1234",
    "type": "mySql",
    "started": STARTED,
    "connectionParameters": {
        "host": "/private/var/folders/qv/w6y030t978518rzpnk1kt0_80000gn/T/pytest-of-nadavgihasi/pytest-29/test_happy_flow0/file.db",
        "port": 1234,
        "database": "/private/var/folders/qv/w6y030t978518rzpnk1kt0_80000gn/T/pytest-of-nadavgihasi/pytest-29/test_happy_flow0/file.db",
        "user": "ng",
    },
    "lambda_container_id": "8e86b65e-45b7-46ac-b924-be89113964b7",
    "id": "e3cac203-50db-43e8-bb6e-07add431edf2",
    "ended": ENDED,
    "response": "very-long-response",
}
SQL_SPAN_METADATA = {
    "parentId": "1234",
    "transactionId": "",
    "region": "UNKNOWN",
    "account": "",
    "info": {"tracer": {"version": "1.1.230"}, "traceId": {"Root": ""}},
    "token": "t_10faa5e13e7844aaa1234",
    "type": "mySql",
    "started": STARTED,
    "connectionParameters": {
        "host": "/private/var/folders/qv/w6y030t978518rzpnk1kt0_80000gn/T/pytest-of-nadavgihasi/pytest-29/test_happy_flow0/file.db",
        "port": 1234,
        "database": "/private/var/folders/qv/w6y030t978518rzpnk1kt0_80000gn/T/pytest-of-nadavgihasi/pytest-29/test_happy_flow0/file.db",
        "user": "ng",
    },
    "lambda_container_id": "8e86b65e-45b7-46ac-b924-be89113964b7",
    "id": "e3cac203-50db-43e8-bb6e-07add431edf2",
    "ended": ENDED,
    "isMetadata": True,
}


@pytest.mark.parametrize("should_try_zip", [True, False])
def test_create_request_body_default(should_try_zip):
    assert _create_request_body(None, [DUMMY_SPAN], False, should_try_zip) == json.dumps(
        [DUMMY_SPAN]
    )


@pytest.mark.parametrize("should_try_zip", [True, False])
def test_create_request_body_not_effecting_small_events(should_try_zip):
    assert _create_request_body(None, [DUMMY_SPAN], True, should_try_zip, 1_000_000) == json.dumps(
        [DUMMY_SPAN]
    )


def test_create_request_body_keep_function_span_and_filter_other_spans(unzip_zipped_spans):
    input_spans = [DUMMY_SPAN, DUMMY_SPAN, DUMMY_SPAN, FUNCTION_END_SPAN, FUNCTION_END_SPAN]
    expected_result = [FUNCTION_END_SPAN_METADATA, FUNCTION_END_SPAN_METADATA]
    size = get_event_base64_size(expected_result)

    result = _create_request_body(None, input_spans, True, False, size)

    assert result == json.dumps(expected_result)

    # With Zipping enabled, we are able to consume all spans
    result = _create_request_body(None, input_spans, True, True, size)
    assert isinstance(result, list)
    assert len(result) > 0
    # unzip the result
    assert unzip_zipped_spans(result[0]) == json.dumps(input_spans)


def test_create_request_body_take_error_first(unzip_zipped_spans):
    expected_result = [FUNCTION_END_SPAN_METADATA, ERROR_HTTP_SPAN_METADATA]
    input_spans = [
        DUMMY_SPAN,
        DUMMY_SPAN,
        DUMMY_SPAN,
        ERROR_HTTP_SPAN,
        FUNCTION_END_SPAN,
    ]
    size = get_event_base64_size(expected_result) + SPANS_SEND_SIZE_ENRICHMENT_SPAN_BUFFER

    result = _create_request_body(
        None, input_spans, True, False, max_size=size, max_error_size=size
    )
    assert result == json.dumps(expected_result)

    # With Zipping enabled, we are able to consume all spans
    result = _create_request_body(None, input_spans, True, True, max_size=size, max_error_size=size)
    print(result)
    assert isinstance(result, list)
    assert len(result) > 0
    # unzip the result
    assert unzip_zipped_spans(result[0]) == json.dumps(input_spans)


def test_create_request_body_take_only_metadata_function_span(caplog):
    expected_result = [FUNCTION_END_SPAN_METADATA]
    input_spans = [FUNCTION_END_SPAN]
    size = get_event_base64_size(expected_result)

    result = _create_request_body(
        None, input_spans, True, False, max_size=size, max_error_size=size
    )

    assert caplog.records[0].message == "Starting smart span selection"
    assert result == json.dumps(expected_result)


@pytest.mark.parametrize(
    ["test_case", "wrapper_span", "wrapper_span_metadata"],
    [
        ["test redis span", REDIS_SPAN, REDIS_SPAN_METADATA],
        ["test http span", HTTP_SPAN, HTTP_SPAN_METADATA],
        ["test pymongo span", PYMONGO_SPAN, PYMONGO_SPAN_METADATA],
        ["test sql span", SQL_SPAN, SQL_SPAN_METADATA],
    ],
)
def test_create_request_body(
    test_case: str, wrapper_span: dict, wrapper_span_metadata: dict, caplog
) -> None:
    expected_result = [
        FUNCTION_END_SPAN,
        {**ENRICHMENT_SPAN_METADATA, "totalSpans": 3},
        wrapper_span_metadata,
    ]
    input_spans = [wrapper_span, {**ENRICHMENT_SPAN, "totalSpans": 3}, FUNCTION_END_SPAN]
    size = get_event_base64_size(expected_result) + SPANS_SEND_SIZE_ENRICHMENT_SPAN_BUFFER

    result = _create_request_body(
        None, input_spans, True, False, max_size=size, max_error_size=size
    )

    assert caplog.records[0].message == "Starting smart span selection"
    assert result == json.dumps(expected_result)


def test_with_many_spans():
    expected_result = [FUNCTION_END_SPAN] + [HTTP_SPAN] * 50 + [HTTP_SPAN_METADATA] * 50
    input_spans = [FUNCTION_END_SPAN] + [HTTP_SPAN] * 100
    size = get_event_base64_size(expected_result) + SPANS_SEND_SIZE_ENRICHMENT_SPAN_BUFFER

    # Without zipping
    result = _create_request_body(
        None, input_spans, True, False, max_size=size, max_error_size=size
    )
    print(result)

    assert result == json.dumps(expected_result)

    # With zipping
    result_with_zip = _create_request_body(
        None, input_spans, True, True, max_size=size, max_error_size=size
    )
    # assert zip result is an array
    assert isinstance(result_with_zip, list)
    # assert zip result is not empty
    assert len(result_with_zip) > 0
    # assert zip result is smaller than the original result
    assert len(result_with_zip[0]) < len(result)

    # We do not support Zipping for China region
    china_result = _create_request_body(
        "cn-northwest-1", input_spans, True, True, max_size=size, max_error_size=size
    )
    assert isinstance(china_result, str)

    # because size is small, zipping does not help
    result_with_zip = _create_request_body(
        None, input_spans, True, True, max_size=size // 100, max_error_size=size // 100
    )
    print(result_with_zip)
    assert isinstance(result_with_zip, str)
    assert len(result_with_zip) <= size // 100


@pytest.mark.parametrize(
    ["arg", "host"],
    [("https://a.com", "a.com"), (f"https://b.com{EDGE_PATH}", "b.com"), ("h.com", "h.com")],
)
def test_get_edge_host(arg, host, monkeypatch):
    monkeypatch.setattr(Configuration, "host", arg)
    assert get_edge_host("region") == host


def test_report_json_extension_spans_mode(monkeypatch, reporter_mock, tmpdir):
    extension_dir = tmpdir.mkdir("tmp")
    monkeypatch.setattr(uuid, "uuid4", lambda *args, **kwargs: "span_name")
    monkeypatch.setattr(CoreConfiguration, "should_report", True)
    monkeypatch.setenv("LUMIGO_USE_TRACER_EXTENSION", "TRUE")
    monkeypatch.setenv("LUMIGO_EXTENSION_SPANS_DIR_KEY", extension_dir)
    mocked_urandom = MagicMock(hex=MagicMock(return_value="my_mocked_data"))
    monkeypatch.setattr(os, "urandom", lambda *args, **kwargs: mocked_urandom)

    start_span = [{"span": "true"}]
    report_json(region=None, msgs=start_span, is_start_span=True)

    spans = []
    size_factor = 100
    for i in range(size_factor):
        spans.append({i: "a" * size_factor})
    report_json(region=None, msgs=spans, is_start_span=False)
    start_path_path = f"{get_extension_dir()}/span_name_span"
    end_path_path = f"{get_extension_dir()}/span_name_end"
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
    monkeypatch.setattr(CoreConfiguration, "should_report", True)
    monkeypatch.setattr(http.client, "HTTPSConnection", Mock())
    http.client.HTTPSConnection("force_reconnect").getresponse.side_effect = errors

    report_json(None, [{"a": "b"}])

    assert caplog.records[-2].levelname == final_log


def test_report_json_fast_failure_after_timeout(monkeypatch, reporter_mock, caplog):
    reporter_mock.side_effect = report_json
    monkeypatch.setattr(Configuration, "host", "host")
    monkeypatch.setattr(CoreConfiguration, "should_report", True)
    monkeypatch.setattr(http.client, "HTTPSConnection", Mock())
    http.client.HTTPSConnection("force_reconnect").getresponse.side_effect = socket.timeout

    assert report_json(None, [{"a": "b"}]) >= 0  # some duration is expected
    # Check if the expected message is in any of the log records
    messages = [record.msg for record in caplog.records]
    assert "Timeout while connecting to host" in messages, "The expected log message was not found"

    assert report_json(None, [{"a": "b"}]) == 0
    assert caplog.records[-1].msg == "Skip sending messages due to previous timeout"

    InternalState.timeout_on_connection = datetime(2016, 1, 1)
    assert report_json(None, [{"a": "b"}]) == 0
    # Check if the expected message is in any of the log records
    messages = [record.msg for record in caplog.records]
    assert "Timeout while connecting to host" in messages, "The expected log message was not found"


def test_report_json_china_missing_access_key_id(monkeypatch, reporter_mock, caplog):
    monkeypatch.setattr(CoreConfiguration, "should_report", True)
    reporter_mock.side_effect = report_json
    assert report_json(CHINA_REGION, [{"a": "b"}]) == 0
    assert any(
        "edge_kinesis_aws_access_key_id" in record.message and record.levelname == "ERROR"
        for record in caplog.records
    )


def test_report_json_china_missing_secret_access_key(monkeypatch, reporter_mock, caplog):
    monkeypatch.setattr(CoreConfiguration, "should_report", True)
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_access_key_id", "my_value")
    reporter_mock.side_effect = report_json
    assert report_json(CHINA_REGION, [{"a": "b"}]) == 0
    assert any(
        "edge_kinesis_aws_secret_access_key" in record.message and record.levelname == "ERROR"
        for record in caplog.records
    )


def test_report_json_china_no_boto(monkeypatch, reporter_mock, caplog):
    reporter_mock.side_effect = report_json
    monkeypatch.setattr(CoreConfiguration, "should_report", True)
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_access_key_id", "my_value")
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_secret_access_key", "my_value")
    monkeypatch.setattr(lambda_reporter, "boto3", None)

    report_json(CHINA_REGION, [{"a": "b"}])

    assert any(
        "boto3 is missing. Unable to send to Kinesis" in record.message
        and record.levelname == "ERROR"  # noqa
        for record in caplog.records
    )


def test_report_json_china_on_error_no_exception_and_notify_user(capsys, monkeypatch):
    monkeypatch.setattr(CoreConfiguration, "should_report", True)
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_access_key_id", "my_value")
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_secret_access_key", "my_value")
    monkeypatch.setattr(boto3, "client", MagicMock(side_effect=Exception))
    lumigo_utils.get_logger().setLevel(logging.CRITICAL)

    report_json(CHINA_REGION, [{"a": "b"}])

    assert "Failed to send spans" in capsys.readouterr().out


def test_china_shouldnt_establish_http_connection(monkeypatch):
    monkeypatch.setenv("AWS_REGION", CHINA_REGION)
    # Reload a duplicate of lambda_reporter
    spec = importlib.util.find_spec("lumigo_tracer.lambda_tracer.lambda_reporter")
    lumigo_utils_reloaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lumigo_utils_reloaded)
    establish_connection()

    assert lumigo_utils_reloaded.edge_connection is None


def test_china_with_env_variable_shouldnt_reuse_boto3_connection(monkeypatch):
    monkeypatch.setenv("LUMIGO_KINESIS_SHOULD_REUSE_CONNECTION", "false")
    monkeypatch.setattr(CoreConfiguration, "should_report", True)
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_access_key_id", "my_value")
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_secret_access_key", "my_value")
    monkeypatch.setattr(boto3, "client", MagicMock())

    report_json(CHINA_REGION, [{"a": "b"}])
    report_json(CHINA_REGION, [{"a": "b"}])

    assert boto3.client.call_count == 2


def test_china_reuse_boto3_connection(monkeypatch):
    monkeypatch.setattr(CoreConfiguration, "should_report", True)
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_access_key_id", "my_value")
    monkeypatch.setattr(Configuration, "edge_kinesis_aws_secret_access_key", "my_value")
    monkeypatch.setattr(boto3, "client", MagicMock())

    report_json(CHINA_REGION, [{"a": "b"}])
    report_json(CHINA_REGION, [{"a": "b"}])

    boto3.client.assert_called_once()


def test_update_enrichment_span_about_prioritized_spans_no_drops():
    enrichment_span = {"type": ENRICHMENT_TYPE, "id": "enrich"}
    span1 = {"type": HTTP_TYPE, "id": "1"}
    span2 = {"type": HTTP_TYPE, "id": "2"}
    spans_dict = {1: enrichment_span, 2: span1, 3: span2}
    msgs = [enrichment_span, span1, span2]
    current_size = sum([get_event_base64_size(s) for s in msgs])
    max_size = current_size
    result = _update_enrichment_span_about_prioritized_spans(
        spans_dict, msgs, current_size, max_size
    )
    assert result == msgs


def test_update_enrichment_span_about_prioritized_spans_with_drops():
    enrichment_span = {"type": ENRICHMENT_TYPE, "id": "enrich"}
    span1 = {"type": HTTP_TYPE, "id": "1"}
    span2 = {"type": HTTP_TYPE, "id": "2"}
    spans_dict = {
        1: enrichment_span,
        2: span1,
        # Dropped span2
    }
    msgs = [enrichment_span, span1, span2]
    current_size = sum([get_event_base64_size(s) for s in spans_dict.values()])
    max_size = current_size * 100
    result = _update_enrichment_span_about_prioritized_spans(
        spans_dict, msgs, current_size, max_size
    )
    assert [s for s in result if s["type"] == HTTP_TYPE and s["id"] == "1"]
    assert [s for s in result if s["type"] == HTTP_TYPE and s["id"] == "2"] == []
    enrichment_spans = [s for s in result if s["type"] == ENRICHMENT_TYPE and s["id"] == "enrich"]
    assert len(enrichment_spans) == 1
    resulting_enrichment_span = enrichment_spans[0]
    assert resulting_enrichment_span == {
        "type": ENRICHMENT_TYPE,
        "id": "enrich",
        "droppedSpansReasons": {"SPANS_SENT_SIZE_LIMIT": {"drops": 1}},
    }


def test_update_enrichment_span_about_prioritized_spans_with_drops_no_size_left_for_dropped_report():
    enrichment_span = {"type": ENRICHMENT_TYPE, "id": "enrich"}
    span1 = {"type": HTTP_TYPE, "id": "1"}
    span2 = {"type": HTTP_TYPE, "id": "2"}
    spans_dict = {
        1: enrichment_span,
        2: span1,
        # Dropped span2
    }
    msgs = [enrichment_span, span1, span2]
    current_size = sum([get_event_base64_size(s) for s in spans_dict.values()])
    max_size = current_size
    result = _update_enrichment_span_about_prioritized_spans(
        spans_dict, msgs, current_size, max_size
    )
    assert [s for s in result if s["type"] == HTTP_TYPE and s["id"] == "1"]
    assert [s for s in result if s["type"] == HTTP_TYPE and s["id"] == "2"] == []
    enrichment_spans = [s for s in result if s["type"] == ENRICHMENT_TYPE and s["id"] == "enrich"]
    assert len(enrichment_spans) == 1
    resulting_enrichment_span = enrichment_spans[0]
    assert resulting_enrichment_span == {"type": ENRICHMENT_TYPE, "id": "enrich"}
    assert sum([get_event_base64_size(s) for s in result]) <= max_size


def test_split_and_zip_spans_successfully():
    MAX_SPANS_BULK_SIZE = 200
    # Create spans list with size 2 * MAX_SPANS_BULK_SIZE
    spans = [{} for _ in range(MAX_SPANS_BULK_SIZE * 2)]

    # Call split_and_zip_spans
    zipped_spans_bulks = _split_and_zip_spans(spans)

    # Test that it splits into the correct number of bulks
    assert len(zipped_spans_bulks) == (MAX_SPANS_BULK_SIZE * 2) // MAX_SPANS_BULK_SIZE

    # Test unzipping and verify that it equals the original spans
    unzipped_spans = []
    for zipped_span in zipped_spans_bulks:
        zipped_span_unload = json.loads(zipped_span)
        # Decode base64 and unzip
        unzipped: str = gzip.decompress(b64decode(zipped_span_unload)).decode("utf-8")
        unzipped_spans.extend(json.loads(unzipped))

    # Check that unzipped spans match the original spans
    assert unzipped_spans == spans

    # Create spans list with size MAX_SPANS_BULK_SIZE / 3
    spans = [{} for _ in range(MAX_SPANS_BULK_SIZE // 3)]

    # Call split_and_zip_spans
    zipped_spans_bulks = _split_and_zip_spans(spans)

    assert len(zipped_spans_bulks) == 1
