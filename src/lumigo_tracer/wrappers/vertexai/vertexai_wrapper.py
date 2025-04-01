import importlib
import uuid
from functools import partial
from typing import Any, Callable, Dict, List, Optional

from lumigo_core.logger import get_logger
from lumigo_core.lumigo_utils import get_current_ms_time

from lumigo_tracer.lambda_tracer.lambda_reporter import VERTEXAI_SPAN
from lumigo_tracer.lambda_tracer.spans_container import SpansContainer
from lumigo_tracer.libs.wrapt import wrap_function_wrapper
from lumigo_tracer.lumigo_utils import lumigo_safe_execute

WRAPPED_METHODS = [
    {
        "package": "vertexai.generative_models",
        "object": "GenerativeModel",
        "method": "generate_content",
        "span_name": "vertexai.generate_content",
        "is_async": False,
    },
    {
        "package": "vertexai.generative_models",
        "object": "GenerativeModel",
        "method": "generate_content_async",
        "span_name": "vertexai.generate_content_async",
        "is_async": True,
    },
    {
        "package": "vertexai.generative_models",
        "object": "ChatSession",
        "method": "send_message",
        "span_name": "vertexai.send_message",
        "is_async": False,
    },
    {
        "package": "vertexai.preview.generative_models",
        "object": "GenerativeModel",
        "method": "generate_content",
        "span_name": "vertexai.generate_content",
        "is_async": False,
    },
    {
        "package": "vertexai.preview.generative_models",
        "object": "GenerativeModel",
        "method": "generate_content_async",
        "span_name": "vertexai.generate_content_async",
        "is_async": True,
    },
    {
        "package": "vertexai.preview.generative_models",
        "object": "ChatSession",
        "method": "send_message",
        "span_name": "vertexai.send_message",
        "is_async": False,
    },
    {
        "package": "vertexai.language_models",
        "object": "TextGenerationModel",
        "method": "predict",
        "span_name": "vertexai.predict",
        "is_async": False,
    },
    {
        "package": "vertexai.language_models",
        "object": "TextGenerationModel",
        "method": "predict_async",
        "span_name": "vertexai.predict_async",
        "is_async": True,
    },
    {
        "package": "vertexai.language_models",
        "object": "TextGenerationModel",
        "method": "predict_streaming",
        "span_name": "vertexai.predict_streaming",
        "is_async": False,
    },
    {
        "package": "vertexai.language_models",
        "object": "TextGenerationModel",
        "method": "predict_streaming_async",
        "span_name": "vertexai.predict_streaming_async",
        "is_async": True,
    },
    {
        "package": "vertexai.language_models",
        "object": "ChatSession",
        "method": "send_message",
        "span_name": "vertexai.send_message",
        "is_async": False,
    },
    {
        "package": "vertexai.language_models",
        "object": "ChatSession",
        "method": "send_message_streaming",
        "span_name": "vertexai.send_message_streaming",
        "is_async": False,
    },
]


def wrap_vertexai_func(
    func: Callable[..., Any],
    instance: Any,
    args: List[Any],
    kwargs: Dict[str, Any],
    func_name: Optional[str] = None,
) -> Any:
    span_id = None
    with lumigo_safe_execute("wrap vertexai func"):
        get_logger().debug("Vertex AI func called")
        llm_model = "unknown"
        if hasattr(instance, "_model_id"):
            llm_model = instance._model_id
        if hasattr(instance, "_model_name"):
            llm_model = instance._model_name.replace("publishers/google/models/", "")

        span_id = str(uuid.uuid4())
        SpansContainer.get_span().add_span(
            {
                "id": span_id,
                "type": VERTEXAI_SPAN,
                "started": get_current_ms_time(),
                "llmModel": llm_model,
                "requestCommand": func_name or "unknown",
            }
        )
        get_logger().debug(f"Vertex AI span started: {span_id}")

    try:
        ret_val = func(*args, **kwargs)
        with lumigo_safe_execute("wrap vertexai func finished"):
            span = SpansContainer.get_span().get_span_by_id(span_id)
            if not span:
                get_logger().warning("VertexAI span ended without a record on its start")
            else:
                span.update({"ended": get_current_ms_time()})
                get_logger().debug(f"Vertex AI span ended: {span_id}")
        return ret_val
    except Exception as e:
        with lumigo_safe_execute("wrap vertexai func failed"):
            get_logger().debug("Vertex AI span ended with exception")
            span = SpansContainer.get_span().get_span_by_id(span_id)
            if not span:
                get_logger().warning("VertexAI span ended without a record on its start")
            else:
                span.update(
                    {"ended": get_current_ms_time(), "error": e.args[0] if e.args else None}
                )
        raise


def wrap_vertexai() -> None:
    with lumigo_safe_execute("wrap vertexai"):
        get_logger().debug("trying to wrap vertexai")
        if importlib.util.find_spec("vertexai"):
            get_logger().debug("wrapping vertexai")
            for wrapped_method in WRAPPED_METHODS:
                wrap_package = wrapped_method.get("package")
                wrap_object = wrapped_method.get("object")
                wrap_method = wrapped_method.get("method")
                span_name = wrapped_method.get("span_name")
                is_async = wrapped_method.get("is_async")
                if not is_async:
                    wrap_function_wrapper(
                        module=wrap_package,
                        name=f"{wrap_object}.{wrap_method}",
                        wrapper=partial(wrap_vertexai_func, func_name=span_name),
                    )
