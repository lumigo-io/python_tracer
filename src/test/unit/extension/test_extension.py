import time

import pytest
from mock import mock_open, patch

from lumigo_tracer.extension.extension import LumigoExtension
from lumigo_tracer.lumigo_utils import LUMIGO_TOKEN_KEY


@pytest.fixture(autouse=True)
def extension(lambda_service):
    return LumigoExtension(lambda_service)


@pytest.fixture
def event():
    return {"requestId": "1-2-3-4"}


def test_first_invocation_send_no_span(mock_linux_files, reporter_mock, extension, event):
    extension.start_new_invocation(event)
    assert reporter_mock.call_count == 0


def test_send_span_on_second_invocation(mock_linux_files, reporter_mock, extension, event):
    extension.start_new_invocation(event)
    extension.start_new_invocation(event)

    assert reporter_mock.call_count == 1
    sent_span = reporter_mock.call_args_list[0][1]["msgs"][0]
    assert sent_span["requestId"] == "1-2-3-4"


def test_dont_send_span_without_token(
    mock_linux_files, reporter_mock, extension, event, monkeypatch
):
    monkeypatch.delenv(LUMIGO_TOKEN_KEY)

    extension.start_new_invocation(event)
    extension.start_new_invocation(event)

    assert reporter_mock.call_count == 0


def test_send_latest_span_on_shutdown(mock_linux_files, reporter_mock, extension, event):
    extension.start_new_invocation(event)
    extension.shutdown()

    assert reporter_mock.call_count == 1
    sent_span = reporter_mock.call_args_list[0][1]["msgs"][0]
    assert sent_span["requestId"] == "1-2-3-4"


def test_validate_span_structure(mock_linux_files, reporter_mock, extension, event):
    extension.start_new_invocation(event)
    extension.shutdown()

    assert reporter_mock.call_count == 1
    sent_span = reporter_mock.call_args_list[0][1]["msgs"][0]
    assert time.time() * 1000 > sent_span.pop("started")
    assert sent_span.pop("cpuUsageTime")[0]["cpu_time"] == 0
    memory_usage = sent_span.pop("memoryUsage")[0]
    assert memory_usage["memory_usage"] == 0
    assert memory_usage["timestamp"]
    assert sent_span == {
        "networkBytesUsed": 0,
        "requestId": "1-2-3-4",
        "token": "t_123",
        "type": "extensionExecutionEnd",
    }


def test_dont_send_span_in_data_failure(reporter_mock, extension, event):
    m = mock_open()
    m().readlines.return_value = ["cpu BAD FORMAT"]
    m().read.return_value = (
        f"IpExt: 0 0 0 0 277959 0 {int(time.time() * 10000)} 1234 0 0 58649349 0 0 0 0 0"
    )
    with patch("lumigo_tracer.extension.extension_utils.open", m):
        extension.start_new_invocation(event)
        extension.shutdown()

    assert reporter_mock.call_count == 0
