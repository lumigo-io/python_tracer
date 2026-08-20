from lumigo_core.logger import get_logger

from lumigo_tracer.lambda_tracer.spans_container import SpansContainer
from lumigo_tracer.libs.wrapt import wrap_function_wrapper
from lumigo_tracer.lumigo_utils import concat_old_body_to_new, lumigo_safe_execute
from lumigo_tracer.wrappers.http.http_data_classes import HttpRequest
from lumigo_tracer.wrappers.http.sync_http_wrappers import (
    add_request_event,
    update_event_response,
)

try:
    import aiohttp
except Exception:
    aiohttp = None  # type: ignore

LUMIGO_SPAN_ID_KEY = "_lumigo_span_id"


def _register_trace_callbacks(trace_config):  # type: ignore[no-untyped-def]
    trace_config.on_request_start.append(on_request_start)
    trace_config.on_request_chunk_sent.append(on_request_chunk_sent)
    trace_config.on_request_end.append(on_request_end)
    trace_config.on_response_chunk_received.append(on_response_chunk_received)
    trace_config.on_request_exception.append(on_request_exception)


def aiohttp_trace_configs_wrapper(trace_config):  # type: ignore[no-untyped-def]
    def aiohttp_session_init_wrapper(func, instance, args, kwargs):  # type: ignore[no-untyped-def]
        with lumigo_safe_execute("aiohttp aiohttp_session_init_wrapper"):
            traces = kwargs.get("trace_configs") or []
            if isinstance(traces, list):
                traces.append(trace_config)
                kwargs.update({"trace_configs": traces})
        return func(*args, **kwargs)

    return aiohttp_session_init_wrapper


async def on_request_start(session, trace_config_ctx, params):  # type: ignore[no-untyped-def]
    with lumigo_safe_execute("aiohttp on_request_start"):
        span = add_request_event(
            span_id=None,
            parse_params=HttpRequest(
                host=params.url.host,
                method=params.method,
                uri=str(params.url),
                headers=dict(params.headers),
                body=b"",
            ),
        )
        setattr(trace_config_ctx, LUMIGO_SPAN_ID_KEY, span["id"])


async def on_request_chunk_sent(session, trace_config_ctx, params):  # type: ignore[no-untyped-def]
    with lumigo_safe_execute("aiohttp on_request_chunk_sent"):
        span_id = getattr(trace_config_ctx, LUMIGO_SPAN_ID_KEY)
        span = SpansContainer.get_span().get_span_by_id(span_id)
        http_info = span.get("info", {}).get("httpInfo", {})  # type: ignore[union-attr]
        http_info["request"]["body"] = concat_old_body_to_new(
            "requestBody", http_info.get("request", {}).get("body"), params.chunk
        )


async def on_request_end(session, trace_config_ctx, params):  # type: ignore[no-untyped-def]
    with lumigo_safe_execute("aiohttp on_request_end"):
        span_id = getattr(trace_config_ctx, LUMIGO_SPAN_ID_KEY)
        update_event_response(
            span_id, params.url.host, params.response.status, dict(params.response.headers), b""
        )


async def on_response_chunk_received(session, trace_config_ctx, params):  # type: ignore[no-untyped-def]
    with lumigo_safe_execute("aiohttp on_response_chunk_received"):
        span_id = getattr(trace_config_ctx, LUMIGO_SPAN_ID_KEY)
        span = SpansContainer.get_span().get_span_by_id(span_id)
        http_info = span.get("info", {}).get("httpInfo", {})  # type: ignore[union-attr]
        http_info["response"]["body"] = concat_old_body_to_new(
            "responseBody", http_info.get("response", {}).get("body"), params.chunk
        )


async def on_request_exception(session, trace_config_ctx, params):  # type: ignore[no-untyped-def]
    with lumigo_safe_execute("aiohttp on_request_exception"):
        span_id = getattr(trace_config_ctx, LUMIGO_SPAN_ID_KEY)
        span = SpansContainer.get_span().get_span_by_id(span_id)
        SpansContainer.add_exception_to_span(span, params.exception, [])  # type: ignore[arg-type]


def wrap_aiohttp():  # type: ignore[no-untyped-def]
    with lumigo_safe_execute("wrap http calls"):
        get_logger().debug("wrapping http requests")
        if aiohttp:
            trace_config = aiohttp.TraceConfig()
            _register_trace_callbacks(trace_config)
            wrap_function_wrapper(
                "aiohttp.client",
                "ClientSession.__init__",
                aiohttp_trace_configs_wrapper(trace_config),
            )
