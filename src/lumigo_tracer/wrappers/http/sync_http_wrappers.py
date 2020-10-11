from datetime import datetime
import http.client
from io import BytesIO
import importlib.util
from typing import Optional

from lumigo_tracer.libs.wrapt import wrap_function_wrapper
from lumigo_tracer.parsing_utils import safe_get_list, recursive_json_join
from lumigo_tracer.lumigo_utils import get_logger, lumigo_safe_execute, ensure_str, Configuration
from lumigo_tracer.spans_container import SpansContainer, MAX_BODY_SIZE
from lumigo_tracer.wrappers.http.http_data_classes import HttpRequest, HttpState
from collections import namedtuple

from lumigo_tracer.wrappers.http.http_parser import get_parser, HTTP_TYPE

_BODY_HEADER_SPLITTER = b"\r\n\r\n"
_FLAGS_HEADER_SPLITTER = b"\r\n"
MAX_READ_SIZE = 1024
LUMIGO_HEADERS_HOOK_KEY = "_lumigo_headers_hook"


HookedData = namedtuple("HookedData", ["headers", "path"])


def add_request_event(parse_params: HttpRequest):
    """
    This function parses an request event and add it to the span.
    """
    parser = get_parser(parse_params.host)()
    msg = parser.parse_request(parse_params)
    HttpState.previous_request = parse_params
    SpansContainer.get_span().add_span(msg)


def add_unparsed_request(parse_params: HttpRequest):
    """
    This function handle the case where we got a request the is not fully formatted as we expected,
    I.e. there isn't '\r\n' in the request data that <i>logically</i> splits the headers from the body.

    In that case, we will consider it as a continuance of the previous request if they got the same url,
        and we didn't get any answer yet.
    """
    last_event = SpansContainer.get_span().get_last_span()
    if last_event:
        if last_event and last_event.get("type") == HTTP_TYPE and HttpState.previous_request:
            if last_event.get("info", {}).get("httpInfo", {}).get("host") == parse_params.host:
                if "response" not in last_event["info"]["httpInfo"]:
                    SpansContainer.get_span().remove_last_span()
                    body = (HttpState.previous_request.body + parse_params.body)[:MAX_BODY_SIZE]
                    add_request_event(HttpState.previous_request.clone(body=body))
                    return
    add_request_event(parse_params.clone(headers=None))


def update_event_response(
    host: Optional[str], status_code: int, headers: dict, body: bytes
) -> None:
    """
    :param host: If None, use the host from the last span, otherwise this is the first chuck and we can empty
                        the aggregated response body
    This function assumes synchronous execution - we update the last http event.
    """
    last_event = SpansContainer.get_span().remove_last_span()
    if last_event:
        if not host:
            host = last_event.get("info", {}).get("httpInfo", {}).get("host", "unknown")
        else:
            HttpState.previous_response_body = b""

        headers = {k.lower(): v for k, v in headers.items()} if headers else {}
        parser = get_parser(host, headers)()  # type: ignore
        if len(HttpState.previous_response_body) < Configuration.max_entry_size:
            HttpState.previous_response_body += body
        update = parser.parse_response(  # type: ignore
            host, status_code, headers, HttpState.previous_response_body  # type: ignore
        )
        SpansContainer.get_span().add_span(recursive_json_join(update, last_event))


#   Wrappers  #


def _http_send_wrapper(func, instance, args, kwargs):
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
                data = data.read(MAX_READ_SIZE)
                args[0].seek(current_pos)

    host, method, headers, body, uri = (
        getattr(instance, "host", None),
        getattr(instance, "_method", None),
        None,
        None,
        None,
    )
    with lumigo_safe_execute("parse request"):
        if isinstance(data, bytes) and _BODY_HEADER_SPLITTER in data:
            headers, body = data.split(_BODY_HEADER_SPLITTER, 1)
            hooked_headers = getattr(instance, LUMIGO_HEADERS_HOOK_KEY, None)
            if hooked_headers and hooked_headers.headers:
                # we will get here only if _headers_reminder_wrapper ran first. remove its traces.
                headers = {k: ensure_str(v) for k, v in hooked_headers.headers.items()}
                uri = f"{host}{hooked_headers.path}"
                setattr(instance, LUMIGO_HEADERS_HOOK_KEY, None)
            elif _FLAGS_HEADER_SPLITTER in headers:
                request_info, headers = headers.split(_FLAGS_HEADER_SPLITTER, 1)
                headers = http.client.parse_headers(BytesIO(headers))
                path_and_query_params = (
                    # Parse path from request info, remove method (GET | POST) and http version (HTTP/1.1)
                    request_info.decode("ascii")
                    .replace(method, "")
                    .replace(instance._http_vsn_str, "")
                    .strip()
                )
                uri = f"{host}{path_and_query_params}"
                host = host or headers.get("Host")
            else:
                headers = None

    with lumigo_safe_execute("add request event"):
        if headers:
            add_request_event(
                HttpRequest(host=host, method=method, uri=uri, headers=headers, body=body)
            )
        else:
            add_unparsed_request(HttpRequest(host=host, method=method, uri=uri, body=data))

    ret_val = func(*args, **kwargs)
    with lumigo_safe_execute("add response event"):
        SpansContainer.get_span().update_event_end_time()
    return ret_val


def _headers_reminder_wrapper(func, instance, args, kwargs):
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


def _requests_wrapper(func, instance, args, kwargs):
    """
    This is the wrapper of the function `requests.request`.
    This function is being wrapped specifically because it initializes the connection by itself and parses the response,
        which creates a gap from the traditional http.client wrapping.
    """
    start_time = datetime.now()
    ret_val = func(*args, **kwargs)
    with lumigo_safe_execute("requests wrapper time updates"):
        SpansContainer.get_span().update_event_times(start_time=start_time)
    return ret_val


def _response_wrapper(func, instance, args, kwargs):
    """
    This is the wrapper of the function that can be called only after that the http request was sent.
    Note that we don't examine the response data because it may change the original behaviour (ret_val.peek()).
    """
    ret_val = func(*args, **kwargs)
    with lumigo_safe_execute("parse response"):
        headers = dict(ret_val.headers.items())
        status_code = ret_val.code
        update_event_response(instance.host, status_code, headers, b"")
    return ret_val


def _read_wrapper(func, instance, args, kwargs):
    """
    This is the wrapper of the function that can be called only after `getresponse` was called.
    """
    ret_val = func(*args, **kwargs)
    if ret_val:
        with lumigo_safe_execute("parse response.read"):
            update_event_response(None, instance.code, dict(instance.headers.items()), ret_val)
    return ret_val


def _read_stream_wrapper(func, instance, args, kwargs):
    ret_val = func(*args, **kwargs)
    return _read_stream_wrapper_generator(ret_val, instance)


def _read_stream_wrapper_generator(stream_generator, instance):
    for partial_response in stream_generator:
        with lumigo_safe_execute("parse response.read_chunked"):
            update_event_response(
                None, instance.status, dict(instance.headers.items()), partial_response
            )
        yield partial_response


def _putheader_wrapper(func, instance, args, kwargs):
    """
    This is the wrapper of the function that called after that the http request was sent.
    Note that we don't examine the response data because it may change the original behaviour (ret_val.peek()).
    """
    kwargs["headers"]["X-Amzn-Trace-Id"] = SpansContainer.get_span().get_patched_root()
    ret_val = func(*args, **kwargs)
    return ret_val


def wrap_http_calls():
    with lumigo_safe_execute("wrap http calls"):
        get_logger().debug("wrapping the http request")
        wrap_function_wrapper("http.client", "HTTPConnection.send", _http_send_wrapper)
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
