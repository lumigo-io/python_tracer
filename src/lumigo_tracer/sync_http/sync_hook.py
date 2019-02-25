from lumigo_tracer.libs.wrapt import wrap_function_wrapper
from lumigo_tracer.utils import config, get_logger
import http.client
from io import BytesIO
import os
import types
from functools import wraps
from lumigo_tracer.spans_container import SpansContainer, EventType


_BODY_HEADER_SPLITTER = b"\r\n\r\n"
_FLAGS_HEADER_SPLITTER = b"\r\n"
_KILL_SWITCH = "LUMIGO_SWITCH_OFF"
already_wrapped = False


def _request_wrapper(func, instance, args, kwargs):
    """
    This is the wrapper of the requests. it parses the http's message to conclude the url, headers, and body.
    Finally, it add an event to the span, and run the wrapped function (http.client.HTTPConnection.send).
    """
    if args and _BODY_HEADER_SPLITTER in args[0]:
        headers, body = args[0].split(_BODY_HEADER_SPLITTER, 1)
        if _FLAGS_HEADER_SPLITTER in headers:
            _, headers = headers.split(_FLAGS_HEADER_SPLITTER, 1)
            headers = http.client.parse_headers(BytesIO(headers))
            url = headers.get("Host")
            SpansContainer.get_span().add_event(url, headers, body, EventType.REQUEST)
            return func(*args, **kwargs)

    SpansContainer.get_span().add_event(None, None, args[0], EventType.REQUEST)
    return func(*args, **kwargs)


def _response_wrapper(func, instance, args, kwargs):
    """
    This is the wrapper of the function that called after that the http request was sent.
    Note that we don't examine the response data because it may change the original behaviour (ret_val.peek()).
    """
    ret_val = func(*args, **kwargs)
    headers = ret_val.headers
    SpansContainer.get_span().update_event_headers(instance.host, headers)
    return ret_val


def _putheader_wrapper(func, instance, args, kwargs):
    """
    This is the wrapper of the function that called after that the http request was sent.
    Note that we don't examine the response data because it may change the original behaviour (ret_val.peek()).
    """
    kwargs["headers"]["X-Amzn-Trace-Id"] = SpansContainer.get_span().get_patched_root()
    ret_val = func(*args, **kwargs)
    return ret_val


def _lumigo_tracer(func):
    @wraps(func)
    def lambda_wrapper(*args, **kwargs):
        if os.environ.get(_KILL_SWITCH):
            return func(*args, **kwargs)

        executed = False

        try:
            wrap_http_calls()
            SpansContainer.create_span(args[1] if args and len(args) > 1 else None)
            try:
                executed = True
                ret_val = func(*args, **kwargs)
            except Exception as e:
                # The case where the lambda raised an exception
                SpansContainer.get_span().add_exception_event(e)
                raise
            finally:
                SpansContainer.get_span().end()
            return ret_val
        except Exception:
            get_logger().exception("exception in the wrapper", exc_info=True)
            # The case where our wrapping raised an exception
            if not executed:
                return func(*args, **kwargs)
            else:
                raise

    return lambda_wrapper


def lumigo_tracer(*args, **kwargs):
    """
    This function should be used as wrapper to your lambda function.
    It will trace your HTTP calls and send it to our backend, which will help you understand it better.

    If the kill switch is activated (env variable `LUMIGO_SWITCH_OFF` set to 1), this function does nothing.

    You can pass to this decorator more configurations to configure the interface to lumigo,
        See `lumigo_tracer.reporter.config` for more details on the available configuration.
    """
    if args and isinstance(args[0], types.FunctionType):
        return _lumigo_tracer(args[0])
    config(*args, **kwargs)
    return _lumigo_tracer


def wrap_http_calls():
    global already_wrapped
    if not already_wrapped:
        get_logger().debug("wrapping the http request")
        wrap_function_wrapper("http.client", "HTTPConnection.send", _request_wrapper)
        wrap_function_wrapper("botocore.awsrequest", "AWSRequest.__init__", _putheader_wrapper)
        wrap_function_wrapper("http.client", "HTTPConnection.getresponse", _response_wrapper)
        already_wrapped = True
