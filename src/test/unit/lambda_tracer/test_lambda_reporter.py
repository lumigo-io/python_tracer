import datetime
import http.client
import importlib.util
import json
import logging
import os
import socket
import uuid
from unittest.mock import Mock

import boto3
import pytest
from mock import MagicMock

from lumigo_tracer import lumigo_utils
from lumigo_tracer.lambda_tracer import lambda_reporter
from lumigo_tracer.lambda_tracer.lambda_reporter import (
    CHINA_REGION,
    EDGE_PATH,
    _create_request_body,
    _get_event_base64_size,
    establish_connection,
    get_edge_host,
    get_extension_dir,
    report_json,
)
from lumigo_tracer.lumigo_utils import Configuration, InternalState


@pytest.fixture
def dummy_span():
    return {"dummy": "dummy"}


@pytest.fixture
def function_end_span():
    return {"dummy_end": "dummy_end"}


@pytest.fixture
def error_span():
    return {"dummy": "dummy", "error": "Error"}


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
    ["arg", "host"],
    [("https://a.com", "a.com"), (f"https://b.com{EDGE_PATH}", "b.com"), ("h.com", "h.com")],
)
def test_get_edge_host(arg, host, monkeypatch):
    monkeypatch.setattr(Configuration, "host", arg)
    assert get_edge_host("region") == host


def test_report_json_extension_spans_mode(monkeypatch, reporter_mock, tmpdir):
    extension_dir = tmpdir.mkdir("tmp")
    monkeypatch.setattr(uuid, "uuid4", lambda *args, **kwargs: "span_name")
    monkeypatch.setattr(Configuration, "should_report", True)
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
    monkeypatch.setattr(lambda_reporter, "boto3", None)

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
    # Reload a duplicate of lambda_reporter
    spec = importlib.util.find_spec("lumigo_tracer.lambda_tracer.lambda_reporter")
    lumigo_utils_reloaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lumigo_utils_reloaded)
    establish_connection()

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
