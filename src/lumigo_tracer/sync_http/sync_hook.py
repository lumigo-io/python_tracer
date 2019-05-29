from lumigo_tracer.libs.wrapt import wrap_function_wrapper
from lumigo_tracer.parsers.utils import safe_get
from lumigo_tracer.utils import config, get_logger, lumigo_safe_execute, is_enhanced_print
import http.client
from io import BytesIO
import os
from functools import wraps
from lumigo_tracer.spans_container import SpansContainer
import builtins
from ..parsers.http_data_classes import HttpRequest

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
    with lumigo_safe_execute("parse requested streams"):
        if isinstance(data, BytesIO):
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
            if _FLAGS_HEADER_SPLITTER in headers:
                _, headers = headers.split(_FLAGS_HEADER_SPLITTER, 1)
                headers = http.client.parse_headers(BytesIO(headers))
                path_and_query_params = (
                    _.decode("ascii")
                    .replace(method, "")
                    .replace(instance._http_vsn_str, "")
                    .strip()
                )
                uri = "{}{}".format(host, path_and_query_params)
                host = host or headers.get("Host")

    with lumigo_safe_execute("add request event"):
        if headers:
            SpansContainer.get_span().add_request_event(
                HttpRequest(host=host, method=method, uri=uri, headers=headers, body=body)
            )
        else:
            SpansContainer.get_span().add_unparsed_request(
                HttpRequest(host=host, method=method, uri=uri, headers=headers, body=data)
            )

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
        response_code = ret_val.code
        SpansContainer.get_span().add_response_event(instance.host, response_code, headers)
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
        local_print = print
        try:

            if is_enhanced_print():
                if len(args) >= 2:
                    request_id = getattr(args[1], "aws_request_id", "")
                    prefix = f"RequestId: {request_id}"
                    builtins.print = lambda *args, **kwargs: local_print(
                        prefix, *[str(arg).replace("\n", f"\n{prefix} ") for arg in args], **kwargs
                    )
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
                builtins.print = local_print
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


class LumigoChalice:
    def __init__(self, app, *args, **kwargs):
        self.original_app_attr_getter = app.__getattribute__
        self.lumigo_app = lumigo_tracer(*args, **kwargs)(app)

    def __getattr__(self, item):
        return self.original_app_attr_getter(item)

    def __call__(self, *args, **kwargs):
        return self.lumigo_app.__call__(*args, **kwargs)


def wrap_http_calls():
    global already_wrapped
    if not already_wrapped:
        with lumigo_safe_execute("wrap http calls"):
            get_logger().debug("wrapping the http request")
            wrap_function_wrapper("http.client", "HTTPConnection.send", _request_wrapper)
            wrap_function_wrapper("botocore.awsrequest", "AWSRequest.__init__", _putheader_wrapper)
            wrap_function_wrapper("http.client", "HTTPConnection.getresponse", _response_wrapper)
            already_wrapped = True
