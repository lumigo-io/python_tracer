import copy
import http.client
import importlib.util
import json
import logging
import os
import socket
import uuid
from datetime import datetime, timedelta
from unittest.mock import Mock

import boto3
import pytest
from lumigo_core.configuration import CoreConfiguration
from mock import MagicMock
from pytest import fixture

from lumigo_tracer import lumigo_utils
from lumigo_tracer.lambda_tracer import lambda_reporter
from lumigo_tracer.lambda_tracer.lambda_reporter import (
    CHINA_REGION,
    EDGE_PATH,
    ENRICHMENT_TYPE,
    FUNCTION_TYPE,
    HTTP_TYPE,
    MONGO_SPAN,
    REDIS_SPAN,
    _create_request_body,
    _get_event_base64_size,
    establish_connection,
    get_edge_host,
    get_extension_dir,
    report_json,
)
from lumigo_tracer.lumigo_utils import Configuration, InternalState


@fixture
def dummy_span() -> dict:
    return {"dummy": "dummy", "type": ENRICHMENT_TYPE}


@fixture
def function_end_span() -> dict:
    return {"dummy_end": "dummy_end", "type": FUNCTION_TYPE, "envs": {"a": "b"}}


@fixture
def function_end_span_metadata() -> dict:
    return {"dummy_end": "dummy_end", "type": FUNCTION_TYPE}


@fixture
def error_span() -> dict:
    return {"dummy": "dummy", "error": "Error", "type": HTTP_TYPE}


@fixture
def redis_span() -> dict:
    now = datetime.now()
    return {
        "type": REDIS_SPAN,
        "id": "77d0b751-9496-4257-b910-25e33c029365",
        "connectionOptions": {"host": "lumigo", "port": None},
        "transactionId": "transaction-id",
        "started": (now - timedelta(seconds=10)).timestamp() * 1000,
        "requestArgs": '[[{"a": 1}], ["a"]]',
        "lambda_container_id": "b7bfabd6-bc95-445a-965d-513922afbcdd",
        "account": "account-id",
        "region": "UNKNOWN",
        "parentId": "parent-id",
        "info": {"tracer": {"version": "1.1.230"}, "traceId": {"Root": ""}},
        "requestCommand": '["SET", "GET"]',
        "token": "t_token",
        "ended": now.timestamp() * 1000,
        "response": '"Result"',
    }


@fixture
def redis_span_metadata(redis_span: dict) -> dict:
    span_copy = copy.deepcopy(redis_span)
    span_copy.pop("requestArgs")
    span_copy.pop("response")
    return span_copy


@fixture
def http_span() -> dict:
    now = datetime.now()
    return {
        "transactionId": "transaction-id",
        "id": "8b32c4b4-e483-4741-9eef-b8f8f6c72f66",
        "started": (now - timedelta(seconds=10)).timestamp() * 1000,
        "info": {
            "tracer": {"version": "1.1.230"},
            "traceId": {"Root": ""},
            "httpInfo": {
                "host": "www.google.com",
                "request": {
                    "headers": '{"host": "www.google.com", "accept-encoding": "identity", "content-length": "0"}',
                    "body": "very interesting body",
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
        "ended": now.timestamp() * 1000,
    }


@fixture
def http_span_metadata(http_span: dict) -> dict:
    span_copy = copy.deepcopy(http_span)
    span_copy["info"]["httpInfo"]["request"].pop("headers")
    span_copy["info"]["httpInfo"]["request"].pop("body")
    return span_copy


@fixture
def pymongo_span() -> dict:
    now = datetime.now()
    return {
        "id": "6c86ff87-07b4-4663-9e2b-15acb75a81f0",
        "mongoOperationId": "oid",
        "type": MONGO_SPAN,
        "mongoConnectionId": "cid",
        "databaseName": "dname",
        "mongoRequestId": "rid",
        "lambda_container_id": "7fa30d38-3aed-4e15-96a7-b53116e2b5fa",
        "account": "account",
        "request": '"cmd"',
        "started": (now - timedelta(seconds=10)).timestamp() * 1000,
        "transactionId": "transaction-id",
        "region": "UNKNOWN",
        "parentId": "parent-id",
        "info": {"tracer": {"version": "1.1.230"}, "traceId": {"Root": ""}},
        "token": "token",
        "commandName": "cname",
        "ended": now.timestamp() * 1000,
        "response": '{"code": 200}',
    }


@fixture
def pymongo_span_metadata(pymongo_span: dict) -> dict:
    span_copy = copy.deepcopy(pymongo_span)
    span_copy.pop("request", None)
    span_copy.pop("response", None)
    return span_copy


@fixture
def sql_span() -> dict:
    now = datetime.now()
    return {
        "parentId": "1234",
        "transactionId": "",
        "values": '["saart"]',
        "region": "UNKNOWN",
        "account": "",
        "query": '"INSERT INTO users (name) VALUES (?)"',
        "info": {"tracer": {"version": "1.1.230"}, "traceId": {"Root": ""}},
        "token": "t_10faa5e13e7844aaa1234",
        "type": "mySql",
        "started": (now - timedelta(seconds=10)).timestamp() * 1000,
        "connectionParameters": {
            "host": "/private/var/folders/qv/w6y030t978518rzpnk1kt0_80000gn/T/pytest-of-nadavgihasi/pytest-29/test_happy_flow0/file.db",
            "port": 1234,
            "database": "/private/var/folders/qv/w6y030t978518rzpnk1kt0_80000gn/T/pytest-of-nadavgihasi/pytest-29/test_happy_flow0/file.db",
            "user": "ng",
        },
        "lambda_container_id": "8e86b65e-45b7-46ac-b924-be89113964b7",
        "id": "e3cac203-50db-43e8-bb6e-07add431edf2",
        "ended": now.timestamp() * 1000,
        "response": "very-long-response",
    }


@fixture
def sql_span_metadata(sql_span: dict) -> dict:
    span_copy = copy.deepcopy(sql_span)
    span_copy.pop("query", None)
    span_copy.pop("values", None)
    span_copy.pop("response", None)
    return span_copy


def test_create_request_body_default(dummy_span: dict):
    assert _create_request_body([dummy_span], False) == json.dumps([dummy_span])


def test_create_request_body_not_effecting_small_events(dummy_span: dict):
    assert _create_request_body([dummy_span], True, 1_000_000) == json.dumps([dummy_span])


def test_create_request_body_keep_function_span_and_filter_other_spans(
    dummy_span: dict, function_end_span: dict
):
    input_spans = [dummy_span, dummy_span, dummy_span, function_end_span, function_end_span]
    expected_result = [function_end_span, function_end_span, dummy_span]
    size = _get_event_base64_size(expected_result)

    result = _create_request_body(input_spans, True, size)
    print(result)

    assert result == json.dumps(expected_result)


def test_create_request_body_take_error_first(
    dummy_span: dict, error_span: dict, function_end_span: dict
):
    expected_result = [function_end_span, error_span, dummy_span, dummy_span]
    input_spans = [
        dummy_span,
        dummy_span,
        dummy_span,
        dummy_span,
        dummy_span,
        error_span,
        function_end_span,
    ]
    size = _get_event_base64_size(expected_result)
    assert _create_request_body(
        input_spans, True, max_size=size, max_error_size=size
    ) == json.dumps(expected_result)


def test_create_request_body_take_only_metadata_function_span(
    function_end_span: dict, function_end_span_metadata: dict
):
    expected_result = [function_end_span_metadata]
    input_spans = [function_end_span]
    size = _get_event_base64_size(expected_result)

    result = _create_request_body(input_spans, True, max_size=size, max_error_size=size)

    assert result == json.dumps(expected_result)


def assert_use_metadata_span_when_needed(
    function_span, wrapper_span, wrapper_span_metadata
) -> None:
    expected_result = [function_span, wrapper_span_metadata]
    input_spans = [wrapper_span, function_span]
    size = _get_event_base64_size(expected_result)

    result = _create_request_body(input_spans, True, max_size=size, max_error_size=size)

    assert result == json.dumps(expected_result)


def test_create_request_body_take_only_metadata_redis_span(
    function_end_span: dict, redis_span: dict, redis_span_metadata: dict
):
    assert_use_metadata_span_when_needed(function_end_span, redis_span, redis_span_metadata)


def test_create_request_body_take_only_metadata_http_span(
    function_end_span: dict, http_span: dict, http_span_metadata: dict
):
    assert_use_metadata_span_when_needed(function_end_span, http_span, http_span_metadata)


def test_create_request_body_take_only_metadata_pymongo_span(
    function_end_span: dict, pymongo_span: dict, pymongo_span_metadata: dict
):
    assert_use_metadata_span_when_needed(function_end_span, pymongo_span, pymongo_span_metadata)


def test_create_request_body_take_only_metadata_sql_span(
    function_end_span: dict, sql_span: dict, sql_span_metadata: dict
):
    assert_use_metadata_span_when_needed(function_end_span, sql_span, sql_span_metadata)


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
        spans.append(
            {
                i: "a" * size_factor,
            }
        )
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

    assert caplog.records[-1].levelname == final_log


def test_report_json_fast_failure_after_timeout(monkeypatch, reporter_mock, caplog):
    reporter_mock.side_effect = report_json
    monkeypatch.setattr(Configuration, "host", "host")
    monkeypatch.setattr(CoreConfiguration, "should_report", True)
    monkeypatch.setattr(http.client, "HTTPSConnection", Mock())
    http.client.HTTPSConnection("force_reconnect").getresponse.side_effect = socket.timeout

    assert report_json(None, [{"a": "b"}]) == 0
    assert caplog.records[-1].msg == "Timeout while connecting to host"

    assert report_json(None, [{"a": "b"}]) == 0
    assert caplog.records[-1].msg == "Skip sending messages due to previous timeout"

    InternalState.timeout_on_connection = datetime(2016, 1, 1)
    assert report_json(None, [{"a": "b"}]) == 0
    assert caplog.records[-1].msg == "Timeout while connecting to host"


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
