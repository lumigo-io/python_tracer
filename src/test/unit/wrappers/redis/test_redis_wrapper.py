from types import SimpleNamespace

import pytest

from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.wrappers.redis.redis_wrapper import execute_command_wrapper, execute_wrapper


@pytest.fixture
def instance():
    return SimpleNamespace(
        connection_pool=SimpleNamespace(connection_kwargs={"Host": "bla", "Port": 5000}),
        command_stack=None,
    )


def func(*args, **kwargs):
    return True


def test_execute_command_wrapper_happy_flow(instance):
    execute_command_wrapper(func, instance, ["SET", {"a": 1}], {})

    spans = SpansContainer.get_span().spans
    assert len(spans) == 1
    assert spans[0]["requestCommand"] == "SET"
    assert spans[0]["requestArgs"] == '{"a": 1}'
    assert spans[0]["ended"] >= spans[0]["started"]
    assert spans[0]["response"] == "true"
    assert "error" not in spans[0]


def test_execute_command_wrapper_failing_command(instance):
    with pytest.raises(ZeroDivisionError):
        execute_command_wrapper(lambda *args, **kwargs: 1 / 0, instance, ["SET", {"a": 1}], {})

    spans = SpansContainer.get_span().spans
    assert len(spans) == 1
    assert spans[0]["requestCommand"] == "SET"
    assert spans[0]["ended"] >= spans[0]["started"]
    assert spans[0]["error"] == "division by zero"
    assert "response" not in spans[0]


def test_execute_command_wrapper_unexpected_params(instance):
    execute_command_wrapper(func, instance, {"not": "list"}, {})

    spans = SpansContainer.get_span().spans
    assert len(spans) == 0


def test_execute_wrapper_happy_flow(instance, monkeypatch):
    monkeypatch.setattr(instance, "command_stack", [["SET", {"a": 1}], ["GET", "a"]])
    execute_wrapper(func, instance, [], {})

    spans = SpansContainer.get_span().spans
    assert len(spans) == 1
    assert spans[0]["requestCommand"] == '["SET", "GET"]'
    assert spans[0]["requestArgs"] == '[{"a": 1}, "a"]'
    assert spans[0]["ended"] >= spans[0]["started"]
    assert spans[0]["response"] == "true"
    assert "error" not in spans[0]


def test_execute_wrapper_failing_command(instance, monkeypatch):
    monkeypatch.setattr(instance, "command_stack", [["SET", {"a": 1}], ["GET", "a"]])
    with pytest.raises(ZeroDivisionError):
        execute_wrapper(lambda *args, **kwargs: 1 / 0, instance, [], {})

    spans = SpansContainer.get_span().spans
    assert len(spans) == 1
    assert spans[0]["requestCommand"] == '["SET", "GET"]'
    assert spans[0]["ended"] >= spans[0]["started"]
    assert spans[0]["error"] == "division by zero"
    assert "response" not in spans[0]


def test_execute_wrapper_unexpected_params(instance, monkeypatch):
    monkeypatch.setattr(instance, "command_stack", [{"not": "list"}])
    execute_wrapper(func, instance, [], {})

    spans = SpansContainer.get_span().spans
    assert len(spans) == 0
