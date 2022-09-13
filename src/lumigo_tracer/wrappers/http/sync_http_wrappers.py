from collections import namedtuple
from typing import Optional, Dict
from datetime import datetime
from io import BytesIO
import importlib.util
import http.client
import logging
import random

from lumigo_tracer.wrappers.http.http_data_classes import HttpRequest, HttpState
from lumigo_tracer.parsing_utils import safe_get_list, recursive_json_join
from lumigo_tracer.wrappers.http.http_parser import get_parser, HTTP_TYPE
from lumigo_tracer.libs.wrapt import wrap_function_wrapper
from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.lumigo_utils import (
    get_logger,
    lumigo_safe_execute,
    ensure_str,
    Configuration,
    lumigo_dumps,
    get_size_upper_bound,
    is_error_code,
    get_edge_host,
    TRUNCATE_SUFFIX,
    concat_old_body_to_new,
    EDGE_SUFFIX,
)

_BODY_HEADER_SPLITTER = b"\r\n\r\n"
_FLAGS_HEADER_SPLITTER = b"\r\n"
LUMIGO_HEADERS_HOOK_KEY = "_lumigo_headers_hook"
LUMIGO_CONNECTION_ID_KEY = "_lumigo_connection_id"


HookedData = namedtuple("HookedData", ["headers", "path"])


def is_lumigo_edge(host: Optional[str]) -> bool:
    if host and (get_edge_host() in host or host.endswith(EDGE_SUFFIX)):
        return True
    return False


def add_request_event(span_id: Optional[str], parse_params: HttpRequest) -> Dict:  # type: ignore[type-arg]
    """
    This function parses an request event and add it to the span.
    """
    if is_lumigo_edge(parse_params.host):
        return {}
    parser = get_parser(parse_params.host)()
    msg = parser.parse_request(parse_params)
    if span_id:
        msg["id"] = span_id
    HttpState.previous_request = parse_params
    new_span = SpansContainer.get_span().add_span(msg)
    HttpState.previous_span_id = new_span["id"]
    return new_span


def add_unparsed_request(span_id: Optional[str], parse_params: HttpRequest) -> Optional[Dict]:  # type: ignore[type-arg]
    """
    This function handle the case where we got a request the is not fully formatted as we expected,
    I.e. there isn't '\r\n' in the request data that <i>logically</i> splits the headers from the body.

    In that case, we will consider it as a continuance of the previous request if they got the same url,
        and we didn't get any answer yet.
    """
    if is_lumigo_edge(parse_params.host):
        return None
    last_event = SpansContainer.get_span().get_span_by_id(span_id)
    if last_event:
        if last_event and last_event.get("type") == HTTP_TYPE:
            http_info = last_event.get("info", {}).get("httpInfo", {})
            if http_info.get("host") == parse_params.host:
                if "response" not in http_info:
                    SpansContainer.get_span().get_span_by_id(span_id)
                    http_info["request"]["body"] = (
                        concat_old_body_to_new(
                            http_info.get("request", {}).get("body"), parse_params.body
                        )
                        if not Configuration.skip_collecting_http_body
                        else ""
                    )
                    if HttpState.previous_span_id == span_id and HttpState.previous_request:
                        HttpState.previous_request.body += parse_params.body
                    return last_event
    return add_request_event(span_id, parse_params)


def update_event_response(
    span_id: str, host: Optional[str], status_code: int, headers: dict, body: bytes  # type: ignore[type-arg]
) -> str:
    """
    :param host: If None, use the host from the last span, otherwise this is the first chuck and we can empty
                        the aggregated response body
    This function assumes synchronous execution - we update the last http event.
    """
    if is_lumigo_edge(host):
        return span_id
    last_event = SpansContainer.get_span().pop_span(span_id)
    if last_event:
        http_info = last_event.get("info", {}).get("httpInfo", {})
        if not host:
            host = http_info.get("host", "unknown")
        old_body = http_info.get("response", {}).get("body")
        if old_body:
            body = concat_old_body_to_new(old_body, body).encode()

        has_error = is_error_code(status_code)
        max_size = Configuration.get_max_entry_size(has_error)
        headers = {k.lower(): v for k, v in headers.items()} if headers else {}
        parser = get_parser(host, headers)()  # type: ignore[arg-type]
        if has_error:
            _update_request_data_increased_size_limit(http_info, max_size)
        update = parser.parse_response(host, status_code, headers, body)  # type: ignore[arg-type]
        SpansContainer.get_span().add_span(recursive_json_join(update, last_event))
        return update.get("id", span_id)
    return span_id


def _update_request_data_increased_size_limit(http_info: dict, max_size: int) -> None:  # type: ignore[type-arg]
    if not HttpState.previous_request or not http_info.get("request"):
        return
    if not HttpState.previous_request.body.startswith(
        http_info["request"].get("body", "").encode()[: len(TRUNCATE_SUFFIX)]
    ):
        return  # this is a different request (non-sync case)
    http_info["request"].update(
        {
            "body": lumigo_dumps(
                HttpState.previous_request.body,
                max_size,
                omit_skip_path=HttpState.omit_skip_path,
            )
            if HttpState.previous_request.body and not Configuration.skip_collecting_http_body
            else "",
            "headers": lumigo_dumps(HttpState.previous_request.headers, max_size),
        }
    )


def get_lumigo_connection_id(instance) -> Optional[int]:  # type: ignore[no-untyped-def]
    return getattr(instance, LUMIGO_CONNECTION_ID_KEY, None)


def set_lumigo_connection_id(instance):  # type: ignore[no-untyped-def]
    setattr(instance, LUMIGO_CONNECTION_ID_KEY, random.random())


#   Wrappers  #


def _http_init_wrapper(func, instance, args, kwargs):  # type: ignore[no-untyped-def]
    """
    We need to attach a unique if to any HTTPConnection, because the runtime sometimes reuse the same connection
    (even to different hosts). See test test_lambda_wrapper_http_non_splitted_send for reference.
    """
    ret_val = func(*args, **kwargs)
    set_lumigo_connection_id(instance)
    return ret_val


def _http_send_wrapper(func, instance, args, kwargs):  # type: ignore[no-untyped-def]
    """
    This is the wrapper of the requests. it parses the http's message to conclude the url, headers, and body.
    Finally, it add an event to the span, and run the wrapped function (http.client.HTTPConnection.send).
    """
    data = safe_get_list(args, 0)
    with lumigo_safe_execute("parse requested streams"):
        if hasattr(data, "read"):
            if not hasattr(data, "seek") or not hasattr(data, "tell"):
                # If we will read this data, then we will change the original behavior
                data = ""
            else:
                current_pos = data.tell()
                data = data.read(get_size_upper_bound())
                args[0].seek(current_pos)

    host, method, headers, body, uri = (
        getattr(instance, "host", None),
        getattr(instance, "_method", None),
        None,
        None,
        None,
    )
    with lumigo_safe_execute("parse request", severity=logging.DEBUG):
        if isinstance(data, bytes) and _BODY_HEADER_SPLITTER in data:
            headers, body = data.split(_BODY_HEADER_SPLITTER, 1)
            hooked_headers = getattr(instance, LUMIGO_HEADERS_HOOK_KEY, None)
            if hooked_headers and hooked_headers.headers:
                # we will get here only if _headers_reminder_wrapper ran first. remove its traces.
                headers = {k: ensure_str(v) for k, v in hooked_headers.headers.items()}  # type: ignore
                uri = f"{host}{hooked_headers.path}"
                setattr(instance, LUMIGO_HEADERS_HOOK_KEY, None)
            elif _FLAGS_HEADER_SPLITTER in headers:
                request_info, headers = headers.split(_FLAGS_HEADER_SPLITTER, 1)
                headers = http.client.parse_headers(BytesIO(headers))  # type: ignore
                path_and_query_params = (
                    # Parse path from request info, remove method (GET | POST) and http version (HTTP/1.1)
                    request_info.decode("ascii")
                    .replace(method, "")  # type: ignore
                    .replace(instance._http_vsn_str, "")
                    .strip()
                )
                uri = f"{host}{path_and_query_params}"
                host = host or headers.get("Host")  # type: ignore
            else:
                headers = None

    span_id = None
    with lumigo_safe_execute("add request event"):
        if headers:
            span = add_request_event(
                span_id=None,
                parse_params=HttpRequest(
                    host=host,
                    method=method,
                    uri=uri,
                    headers=headers,
                    body=body,
                    instance_id=id(instance),
                ),
            )
        else:
            span = add_unparsed_request(  # type: ignore
                HttpState.request_id_to_span_id.get(get_lumigo_connection_id(instance)),  # type: ignore[arg-type]
                HttpRequest(host=host, method=method, uri=uri, body=data, instance_id=id(instance)),
            )
        span_id = span["id"] if span else None
        if span_id:
            HttpState.request_id_to_span_id[get_lumigo_connection_id(instance)] = span_id  # type: ignore[index]

    ret_val = func(*args, **kwargs)
    with lumigo_safe_execute("add response event"):
        if span_id:
            SpansContainer.get_span().update_event_end_time(span_id)
    return ret_val


def _headers_reminder_wrapper(func, instance, args, kwargs):  # type: ignore[no-untyped-def]
    """
    This is the wrapper of the function `http.client.HTTPConnection.request` that gets the headers.
    Remember the headers helps us to improve performances on requests that use this flow.
    """
    with lumigo_safe_execute("add hooked data"):
        setattr(
            instance,
            LUMIGO_HEADERS_HOOK_KEY,
            HookedData(headers=kwargs.get("headers"), path=args[1]),
        )
    return func(*args, **kwargs)


def _requests_wrapper(func, instance, args, kwargs):  # type: ignore[no-untyped-def]
    """
    This is the wrapper of the function `requests.request`.
    This function is being wrapped specifically because it initializes the connection by itself and parses the response,
        which creates a gap from the traditional http.client wrapping.
    Moreover, these "extra" steps may raise exceptions. We should attach the error to the http span.
    """
    start_time = datetime.now()
    try:
        ret_val = func(*args, **kwargs)
    except Exception as exception:
        with lumigo_safe_execute("requests wrapper exception occurred"):
            method = safe_get_list(args, 0, kwargs.get("method", "")).upper()
            url = safe_get_list(args, 1, kwargs.get("url"))
            if Configuration.is_sync_tracer:
                if HttpState.previous_request:
                    span = SpansContainer.get_span().get_span_by_id(HttpState.previous_span_id)
                else:
                    span = add_request_event(
                        None,
                        HttpRequest(
                            host=url,
                            method=method,
                            uri=url,
                            body=kwargs.get("data"),
                            headers=kwargs.get("headers"),
                            instance_id=id(instance),
                        ),
                    )
                    span_id = span["id"]
                    HttpState.request_id_to_span_id[get_lumigo_connection_id(instance)] = span_id  # type: ignore[index]
                SpansContainer.add_exception_to_span(span, exception, [])  # type: ignore[arg-type]
        raise
    with lumigo_safe_execute("requests wrapper time updates"):
        span_id = HttpState.response_id_to_span_id.get(
            get_lumigo_connection_id(ret_val.raw._original_response)  # type: ignore[arg-type]
        )
        SpansContainer.get_span().update_event_times(span_id, start_time=start_time)
    return ret_val


def _response_wrapper(func, instance, args, kwargs):  # type: ignore[no-untyped-def]
    """
    This is the wrapper of the function that can be called only after that the http request was sent.
    Note that we don't examine the response data because it may change the original behaviour (ret_val.peek()).
    """
    ret_val = func(*args, **kwargs)
    with lumigo_safe_execute("parse response"):
        span_id = HttpState.request_id_to_span_id.get(get_lumigo_connection_id(instance))  # type: ignore[arg-type]
        set_lumigo_connection_id(ret_val)
        HttpState.response_id_to_span_id[get_lumigo_connection_id(ret_val)] = span_id  # type: ignore[index,assignment]
        headers = dict(ret_val.headers.items())
        status_code = ret_val.code
        new_span_id = update_event_response(span_id, instance.host, status_code, headers, b"")  # type: ignore[arg-type]
        HttpState.response_id_to_span_id[get_lumigo_connection_id(ret_val)] = new_span_id  # type: ignore[index]
    return ret_val


def _read_wrapper(func, instance, args, kwargs):  # type: ignore[no-untyped-def]
    """
    This is the wrapper of the function that can be called only after `getresponse` was called.
    """
    ret_val = func(*args, **kwargs)
    if ret_val:
        with lumigo_safe_execute("parse response.read"):
            span_id = HttpState.response_id_to_span_id.get(get_lumigo_connection_id(instance))  # type: ignore[arg-type]
            update_event_response(
                span_id, None, instance.code, dict(instance.headers.items()), ret_val  # type: ignore[arg-type]
            )
    return ret_val


def _read_stream_wrapper(func, instance, args, kwargs):  # type: ignore[no-untyped-def]
    ret_val = func(*args, **kwargs)
    return _read_stream_wrapper_generator(ret_val, instance)


def _read_stream_wrapper_generator(stream_generator, instance):  # type: ignore[no-untyped-def]
    for partial_response in stream_generator:
        with lumigo_safe_execute("parse response.read_chunked"):
            span_id = HttpState.response_id_to_span_id.get(
                get_lumigo_connection_id(instance._original_response)  # type: ignore[arg-type]
            )
            update_event_response(
                span_id, None, instance.status, dict(instance.headers.items()), partial_response  # type: ignore[arg-type]
            )
        yield partial_response


def _putheader_wrapper(func, instance, args, kwargs):  # type: ignore[no-untyped-def]
    """
    This is the wrapper of the function that called after that the http request was sent.
    Note that we don't examine the response data because it may change the original behaviour (ret_val.peek()).
    """
    if SpansContainer.get_span().can_path_root():
        kwargs["headers"]["X-Amzn-Trace-Id"] = SpansContainer.get_span().get_patched_root()
    ret_val = func(*args, **kwargs)
    return ret_val


def wrap_http_calls():  # type: ignore[no-untyped-def]
    with lumigo_safe_execute("wrap http calls"):
        get_logger().debug("wrapping http requests")
        wrap_function_wrapper("http.client", "HTTPConnection.send", _http_send_wrapper)
        wrap_function_wrapper("http.client", "HTTPConnection.__init__", _http_init_wrapper)
        wrap_function_wrapper("http.client", "HTTPConnection.request", _headers_reminder_wrapper)
        if importlib.util.find_spec("botocore"):
            wrap_function_wrapper("botocore.awsrequest", "AWSRequest.__init__", _putheader_wrapper)
        wrap_function_wrapper("http.client", "HTTPConnection.getresponse", _response_wrapper)
        wrap_function_wrapper("http.client", "HTTPResponse.read", _read_wrapper)
        if importlib.util.find_spec("urllib3"):
            wrap_function_wrapper(
                "urllib3.response", "HTTPResponse.read_chunked", _read_stream_wrapper
            )
        if importlib.util.find_spec("requests"):
            wrap_function_wrapper("requests.api", "request", _requests_wrapper)
            wrap_function_wrapper("requests", "request", _requests_wrapper)
