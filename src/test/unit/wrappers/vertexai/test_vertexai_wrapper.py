from types import SimpleNamespace

import pytest

from lumigo_tracer.lambda_tracer.spans_container import SpansContainer
from lumigo_tracer.wrappers.vertexai.vertexai_wrapper import wrap_vertexai_func

VERTEXAI_INSTANCE = SimpleNamespace(_model_id="model_name")


def dummy_func(*args, **kwargs):
    return "dummy response"


def test_vertexai_wrapper_happy_flow():
    response = wrap_vertexai_func(dummy_func, VERTEXAI_INSTANCE, [], {}, func_name="dummy_func")
    assert response == "dummy response"
    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 1
    span = spans[0]
    assert span["requestCommand"] == "dummy_func"
    assert span["llmModel"] == "model_name"
    assert span["ended"] >= span["started"]
    assert "error" not in span


def test_vertexai_wrapper_happy_flow_with_args_and_kwargs():
    response = wrap_vertexai_func(
        dummy_func,
        VERTEXAI_INSTANCE,
        [0, "1", True, None, {}],
        {"a": 1, "b": "2", "c": False, "d": None, "e": {}},
        func_name="dummy_func",
    )
    assert response == "dummy response"
    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 1
    span = spans[0]
    assert span["requestCommand"] == "dummy_func"
    assert span["llmModel"] == "model_name"
    assert span["ended"] >= span["started"]
    assert "error" not in span


def test_vertexai_wrapper_exception():
    error_msg = "crash"

    def crashing_func(*args, **kwargs):
        raise Exception(error_msg)

    with pytest.raises(Exception):
        wrap_vertexai_func(crashing_func, VERTEXAI_INSTANCE, [], {}, func_name="dummy_func")
    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 1
    span = spans[0]
    assert span["requestCommand"] == "dummy_func"
    assert span["llmModel"] == "model_name"
    assert span["ended"] >= span["started"]
    assert span["error"] == error_msg


def test_vertexai_wrapper_no_instance():
    response = wrap_vertexai_func(dummy_func, None, [], {}, func_name="dummy_func")
    assert response == "dummy response"
    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 1
    span = spans[0]
    assert span["requestCommand"] == "dummy_func"
    assert span["llmModel"] == "unknown"
    assert span["ended"] >= span["started"]
    assert "error" not in span


def test_vertexai_wrapper_no_model_id():
    instance = SimpleNamespace()
    response = wrap_vertexai_func(dummy_func, instance, [], {}, func_name="dummy_func")
    assert response == "dummy response"
    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 1
    span = spans[0]
    assert span["requestCommand"] == "dummy_func"
    assert span["llmModel"] == "unknown"
    assert span["ended"] >= span["started"]
    assert "error" not in span


def test_vertexai_wrapper_model_name_parsing():
    instance = SimpleNamespace(_model_name="publishers/google/models/model_id")
    response = wrap_vertexai_func(dummy_func, instance, [], {}, func_name="dummy_func")
    assert response == "dummy response"
    spans = list(SpansContainer.get_span().spans.values())
    assert len(spans) == 1
    span = spans[0]
    assert span["requestCommand"] == "dummy_func"
    assert span["llmModel"] == "model_id"
    assert span["ended"] >= span["started"]
    assert "error" not in span
