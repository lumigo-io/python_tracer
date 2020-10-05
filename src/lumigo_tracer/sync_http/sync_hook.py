from datetime import datetime
import http.client
from io import BytesIO
import importlib.util

from lumigo_tracer.libs.wrapt import wrap_function_wrapper
from lumigo_tracer.parsing_utils import safe_get_list
from lumigo_tracer.lumigo_utils import get_logger, lumigo_safe_execute, ensure_str
from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.parsers.http_data_classes import HttpRequest
from collections import namedtuple

_BODY_HEADER_SPLITTER = b"\r\n\r\n"
_FLAGS_HEADER_SPLITTER = b"\r\n"
MAX_READ_SIZE = 1024
already_wrapped = False
LUMIGO_HEADERS_HOOK_KEY = "_lumigo_headers_hook"


HookedData = namedtuple("HookedData", ["headers", "path"])


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
            SpansContainer.get_span().add_request_event(
                HttpRequest(host=host, method=method, uri=uri, headers=headers, body=body)
            )
        else:
            SpansContainer.get_span().add_unparsed_request(
                HttpRequest(host=host, method=method, uri=uri, body=data)
            )

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
        SpansContainer.get_span().update_event_response(instance.host, status_code, headers, b"")
    return ret_val


def _read_wrapper(func, instance, args, kwargs):
    """
    This is the wrapper of the function that can be called only after `getresponse` was called.
    """
    ret_val = func(*args, **kwargs)
    if ret_val:
        with lumigo_safe_execute("parse response.read"):
            SpansContainer.get_span().update_event_response(
                None, instance.code, dict(instance.headers.items()), ret_val
            )
    return ret_val


def _read_stream_wrapper(func, instance, args, kwargs):
    ret_val = func(*args, **kwargs)
    return _read_stream_wrapper_generator(ret_val, instance)


def _read_stream_wrapper_generator(stream_generator, instance):
    for partial_response in stream_generator:
        with lumigo_safe_execute("parse response.read_chunked"):
            SpansContainer.get_span().update_event_response(
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
    global already_wrapped
    if not already_wrapped:
        with lumigo_safe_execute("wrap http calls"):
            get_logger().debug("wrapping the http request")
            wrap_function_wrapper("http.client", "HTTPConnection.send", _http_send_wrapper)
            wrap_function_wrapper(
                "http.client", "HTTPConnection.request", _headers_reminder_wrapper
            )
            if importlib.util.find_spec("botocore"):
                wrap_function_wrapper(
                    "botocore.awsrequest", "AWSRequest.__init__", _putheader_wrapper
                )
            wrap_function_wrapper("http.client", "HTTPConnection.getresponse", _response_wrapper)
            wrap_function_wrapper("http.client", "HTTPResponse.read", _read_wrapper)
            if importlib.util.find_spec("urllib3"):
                wrap_function_wrapper(
                    "urllib3.response", "HTTPResponse.read_chunked", _read_stream_wrapper
                )
            if importlib.util.find_spec("requests"):
                wrap_function_wrapper("requests.api", "request", _requests_wrapper)
            already_wrapped = True
