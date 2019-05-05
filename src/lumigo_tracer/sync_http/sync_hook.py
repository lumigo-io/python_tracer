from lumigo_tracer.libs.wrapt import wrap_function_wrapper
from lumigo_tracer.parsers.utils import safe_get
from lumigo_tracer.utils import config, get_logger, lumigo_safe_execute
import http.client
from io import BytesIO, StringIO
import os
from functools import wraps
from lumigo_tracer.spans_container import SpansContainer, EventType


_BODY_HEADER_SPLITTER = b"\r\n\r\n"
_FLAGS_HEADER_SPLITTER = b"\r\n"
_KILL_SWITCH = "LUMIGO_SWITCH_OFF"
MAX_READ_SIZE = 1024
already_wrapped = False


def _request_wrapper(func, instance, args, kwargs):
    """
    This is the wrapper of the requests. it parses the http's message to conclude the url, headers, and body.
    Finally, it add an event to the span, and run the wrapped function (http.client.HTTPConnection.send).
    """
    data = safe_get(args, 0)
    if isinstance(data, (BytesIO, StringIO)):
        current_pos = data.tell()
        data = data.read(MAX_READ_SIZE)
        args[0].seek(current_pos)

    url, headers, body = getattr(instance, "host", None), None, None
    with lumigo_safe_execute("parse request"):
        if isinstance(data, (str, bytes)) and _BODY_HEADER_SPLITTER in data:
            headers, body = data.split(_BODY_HEADER_SPLITTER, 1)
            if _FLAGS_HEADER_SPLITTER in headers:
                _, headers = headers.split(_FLAGS_HEADER_SPLITTER, 1)
                headers = http.client.parse_headers(BytesIO(headers))
                url = url or headers.get("Host")

    with lumigo_safe_execute("add request event"):
        SpansContainer.get_span().add_event(url, headers, body, EventType.REQUEST)

    ret_val = func(*args, **kwargs)
    with lumigo_safe_execute("add response event"):
        SpansContainer.get_span().update_event_end_time()
    return ret_val


def _response_wrapper(func, instance, args, kwargs):
    """
    This is the wrapper of the function that called after that the http request was sent.
    Note that we don't examine the response data because it may change the original behaviour (ret_val.peek()).
    """
    ret_val = func(*args, **kwargs)
    with lumigo_safe_execute("parse response"):
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
        if str(os.environ.get(_KILL_SWITCH, "")).lower() == "true":
            return func(*args, **kwargs)

        executed = False
        ret_val = None
        try:
            SpansContainer.create_span(*args, force=True)
            SpansContainer.get_span().start()
            wrap_http_calls()
            try:
                executed = True
                ret_val = func(*args, **kwargs)
            except Exception as e:
                # The case where the lambda raised an exception
                SpansContainer.get_span().add_exception_event(e)
                raise
            finally:
                SpansContainer.get_span().end(ret_val)
            return ret_val
        except Exception:
            # The case where our wrapping raised an exception
            if not executed:
                get_logger().exception("exception in the wrapper", exc_info=True)
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
    config(*args, **kwargs)
    return _lumigo_tracer


def wrap_http_calls():
    global already_wrapped
    if not already_wrapped:
        with lumigo_safe_execute("wrap http calls"):
            get_logger().debug("wrapping the http request")
            wrap_function_wrapper("http.client", "HTTPConnection.send", _request_wrapper)
            wrap_function_wrapper("botocore.awsrequest", "AWSRequest.__init__", _putheader_wrapper)
            wrap_function_wrapper("http.client", "HTTPConnection.getresponse", _response_wrapper)
            already_wrapped = True
