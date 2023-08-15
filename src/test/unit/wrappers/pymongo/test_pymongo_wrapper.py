from types import SimpleNamespace

import pytest

from lumigo_tracer.lambda_tracer.spans_container import SpansContainer
from lumigo_tracer.wrappers.pymongo.pymongo_wrapper import LumigoMongoMonitoring


@pytest.fixture
def start_event():
    return SimpleNamespace(
        database_name="dname",
        command_name="cname",
        command="cmd",
        request_id="rid",
        operation_id="oid",
        connection_id="cid",
    )


@pytest.fixture
def success_event():
    return SimpleNamespace(duration_micros=5000, reply={"code": 200}, request_id="rid")


@pytest.fixture
def fail_event():
    return SimpleNamespace(duration_micros=5000, failure={"code": 500}, request_id="rid")


def test_pymongo_happy_flow(monkeypatch, start_event, success_event):
    monitor = LumigoMongoMonitoring()
    monitor.started(start_event)
    monitor.succeeded(success_event)

    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 1
    assert spans[0]["request"] == '"cmd"'
    assert spans[0]["ended"] > spans[0]["started"]
    assert spans[0]["response"] == '{"code": 200}'
    assert "error" not in spans[0]


def test_pymongo_only_start(monkeypatch, start_event):
    monitor = LumigoMongoMonitoring()
    monitor.started(start_event)

    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 1
    assert spans[0]["request"] == '"cmd"'
    assert "duration" not in spans[0]


def test_pymongo_error(monkeypatch, start_event, fail_event):
    monitor = LumigoMongoMonitoring()
    monitor.started(start_event)
    monitor.failed(fail_event)

    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 1
    assert spans[0]["request"] == '"cmd"'
    assert spans[0]["ended"] > spans[0]["started"]
    assert spans[0]["error"] == '{"code": 500}'
    assert "response" not in spans[0]


def test_pymongo_concurrent_events(monkeypatch, start_event, success_event):
    monitor = LumigoMongoMonitoring()
    monitor.started(start_event)
    monitor.started(
        SimpleNamespace(
            database_name="dname",
            command_name="cname",
            command="cmd",
            request_id="rid2",
            operation_id="oid",
            connection_id="cid",
        )
    )
    monitor.succeeded(success_event)

    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 2
    assert spans[0]["mongoRequestId"] == "rid"
    assert spans[0]["ended"] > spans[0]["started"]
    assert spans[0]["response"] == '{"code": 200}'

    assert spans[1]["mongoRequestId"] == "rid2"
    assert "ended" not in spans[1]
