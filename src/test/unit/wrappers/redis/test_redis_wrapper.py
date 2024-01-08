from datetime import datetime
from types import SimpleNamespace

import pytest

from lumigo_tracer.lambda_tracer.spans_container import SpansContainer
from lumigo_tracer.wrappers.redis.redis_wrapper import (
    execute_command_wrapper,
    execute_wrapper,
)

FUNCTION_RESULT = "Result"


@pytest.fixture
def instance() -> SimpleNamespace:
    return SimpleNamespace(
        connection_pool=SimpleNamespace(connection_kwargs={"host": "lumigo"}), command_stack=None
    )


def func(*args, **kwargs) -> str:
    return FUNCTION_RESULT


def test_execute_command_wrapper_happy_flow(instance: SimpleNamespace):
    result = execute_command_wrapper(func, instance, ["SET", {"a": 1}, "b"], {})

    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 1
    assert spans[0]["requestCommand"] == "SET"
    assert spans[0]["requestArgs"] == '[{"a": 1}, "b"]'
    assert spans[0]["connectionOptions"] == {"host": "lumigo", "port": None}
    assert spans[0]["ended"] >= spans[0]["started"]
    assert spans[0]["response"] == '"Result"'
    assert "error" not in spans[0]
    assert result == FUNCTION_RESULT


def test_execute_command_wrapper_non_json(instance: SimpleNamespace):
    result = execute_command_wrapper(
        lambda *args, **kwargs: datetime.now(), instance, ["SET", {"a": 1}, "b"], {}
    )

    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 1
    assert spans[0]["requestCommand"] == "SET"
    assert spans[0]["requestArgs"] == '[{"a": 1}, "b"]'
    assert spans[0]["connectionOptions"] == {"host": "lumigo", "port": None}
    assert spans[0]["ended"] >= spans[0]["started"]
    assert spans[0]["response"]
    assert "error" not in spans[0]
    assert isinstance(result, datetime)


def test_execute_command_wrapper_failing_command(instance: SimpleNamespace):
    with pytest.raises(ZeroDivisionError):
        execute_command_wrapper(lambda *args, **kwargs: 1 / 0, instance, ["SET", {"a": 1}], {})

    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 1
    assert spans[0]["requestCommand"] == "SET"
    assert spans[0]["ended"] >= spans[0]["started"]
    assert spans[0]["error"] == "division by zero"
    assert "response" not in spans[0]


def test_execute_command_wrapper_unexpected_params(instance: SimpleNamespace):
    result = execute_command_wrapper(func, instance, {"not": "list"}, {})

    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 0
    assert result == FUNCTION_RESULT


def test_execute_wrapper_happy_flow(instance: SimpleNamespace, monkeypatch):
    monkeypatch.setattr(instance, "command_stack", [["SET", {"a": 1}], ["GET", "a"]])
    execute_wrapper(func, instance, [], {})

    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 1
    assert spans[0]["requestCommand"] == '["SET", "GET"]'
    assert spans[0]["requestArgs"] == '[[{"a": 1}], ["a"]]'
    assert spans[0]["ended"] >= spans[0]["started"]
    assert spans[0]["response"] == '"Result"'
    assert "error" not in spans[0]


def test_execute_wrapper_failing_command(instance: SimpleNamespace, monkeypatch):
    monkeypatch.setattr(instance, "command_stack", [["SET", {"a": 1}], ["GET", "a"]])
    with pytest.raises(ZeroDivisionError):
        execute_wrapper(lambda *args, **kwargs: 1 / 0, instance, [], {})

    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 1
    assert spans[0]["requestCommand"] == '["SET", "GET"]'
    assert spans[0]["ended"] >= spans[0]["started"]
    assert spans[0]["error"] == "division by zero"
    assert "response" not in spans[0]


def test_execute_wrapper_unexpected_params(instance: SimpleNamespace, monkeypatch):
    monkeypatch.setattr(instance, "command_stack", [{"not": "list"}])
    result = execute_wrapper(func, instance, [], {})

    spans = SpansContainer.get_span().spans
    assert len(spans) == 0
    assert result == FUNCTION_RESULT
