import inspect
import logging
import http.client
from io import BytesIO
import os
import builtins
from functools import wraps
import importlib.util
import botocore.awsrequest  # noqa: F401

from lumigo_tracer.auto_tag.auto_tag_event import AutoTagEvent
from lumigo_tracer.libs.wrapt import wrap_function_wrapper
from lumigo_tracer.parsers.utils import safe_get_list
from lumigo_tracer.utils import (
    config,
    Configuration,
    get_logger,
    lumigo_safe_execute,
    is_aws_environment,
    ensure_str,
)
from lumigo_tracer.spans_container import SpansContainer, TimeoutMechanism
from lumigo_tracer.parsers.http_data_classes import HttpRequest

_BODY_HEADER_SPLITTER = b"\r\n\r\n"
_FLAGS_HEADER_SPLITTER = b"\r\n"
_KILL_SWITCH = "LUMIGO_SWITCH_OFF"
CONTEXT_WRAPPED_BY_LUMIGO_KEY = "_wrapped_by_lumigo"
MAX_READ_SIZE = 1024
already_wrapped = False
LUMIGO_HEADERS_HOOK_KEY = "_lumigo_headers_hook"


def _request_wrapper(func, instance, args, kwargs):
    """
    This is the wrapper of the requests. it parses the http's message to conclude the url, headers, and body.
    Finally, it add an event to the span, and run the wrapped function (http.client.HTTPConnection.send).
    """
    data = safe_get_list(args, 0)
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
            hooked_headers = getattr(instance, LUMIGO_HEADERS_HOOK_KEY, None)
            if hooked_headers:
                # we will get here only if _headers_reminder_wrapper ran first. remove its traces.
                headers = {k: ensure_str(v) for k, v in hooked_headers.items()}
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
    setattr(instance, LUMIGO_HEADERS_HOOK_KEY, kwargs.get("headers"))
    return func(*args, **kwargs)


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


def _is_context_already_wrapped(*args) -> bool:
    """
    This function is here in order to validate that we didn't already wrap this lambda
        (using the sls plugin / auto instrumentation / etc.)
    """
    return len(args) >= 2 and hasattr(args[1], CONTEXT_WRAPPED_BY_LUMIGO_KEY)


def _add_wrap_flag_to_context(*args):
    """
    This function is here in order to validate that we didn't already wrap this invocation
        (using the sls plugin / auto instrumentation / etc.).
    We are adding lumigo's flag to the context, and check it's value in _is_context_already_wrapped.
    """
    if len(args) >= 2:
        with lumigo_safe_execute("wrap context"):
            setattr(args[1], CONTEXT_WRAPPED_BY_LUMIGO_KEY, True)


def _lumigo_tracer(func):
    @wraps(func)
    def lambda_wrapper(*args, **kwargs):
        if str(os.environ.get(_KILL_SWITCH, "")).lower() == "true":
            return func(*args, **kwargs)

        if _is_context_already_wrapped(*args):
            return func(*args, **kwargs)
        _add_wrap_flag_to_context(*args)
        executed = False
        ret_val = None
        local_print = print
        local_logging_format = logging.Formatter.format
        try:
            if Configuration.enhanced_print:
                _enhance_output(args, local_print, local_logging_format)
            SpansContainer.create_span(*args, force=True)
            with lumigo_safe_execute("auto tag"):
                AutoTagEvent.auto_tag_event(args[0])
            SpansContainer.get_span().start(*args)
            wrap_http_calls()
            try:
                executed = True
                ret_val = func(*args, **kwargs)
            except Exception as e:
                with lumigo_safe_execute("Customer's exception"):
                    SpansContainer.get_span().add_exception_event(e, inspect.trace())
                raise
            finally:
                SpansContainer.get_span().end(ret_val)
                if Configuration.enhanced_print:
                    builtins.print = local_print
                    logging.Formatter.format = local_logging_format
            return ret_val
        except Exception:
            # The case where our wrapping raised an exception
            if not executed:
                TimeoutMechanism.stop()
                get_logger().exception("exception in the wrapper", exc_info=True)
                return func(*args, **kwargs)
            else:
                raise

    return lambda_wrapper


def _enhance_output(args, local_print, local_logging_format):
    if len(args) < 2:
        return
    request_id = getattr(args[1], "aws_request_id", "")
    prefix = f"RequestId: {request_id}"
    builtins.print = lambda *args, **kwargs: local_print(
        *[_add_prefix_for_each_line(prefix, str(arg)) for arg in args], **kwargs
    )
    logging.Formatter.format = lambda self, record: _add_prefix_for_each_line(
        prefix, local_logging_format(self, record)
    )


def _add_prefix_for_each_line(prefix: str, text: str):
    enhanced_lines = []
    for line in text.split("\n"):
        if line and not line.startswith(prefix):
            line = prefix + " " + line
        enhanced_lines.append(line)
    return "\n".join(enhanced_lines)


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
    DECORATORS_OF_NEW_HANDLERS = [
        "on_s3_event",
        "on_sns_message",
        "on_sqs_message",
        "schedule",
        "authorizer",
        "lambda_function",
    ]

    def __init__(self, app, *args, **kwargs):
        self.lumigo_conf_args = args
        self.lumigo_conf_kwargs = kwargs
        self.app = app
        self.original_app_attr_getter = app.__getattribute__
        self.lumigo_app = lumigo_tracer(*self.lumigo_conf_args, **self.lumigo_conf_kwargs)(app)

    def __getattr__(self, item):
        original_attr = self.original_app_attr_getter(item)
        if is_aws_environment() and item in self.DECORATORS_OF_NEW_HANDLERS:

            def get_decorator(*args, **kwargs):
                # calling the annotation, example `app.authorizer(THIS)`
                chalice_actual_decorator = original_attr(*args, **kwargs)

                def wrapper2(func):
                    user_func_wrapped_by_chalice = chalice_actual_decorator(func)
                    return LumigoChalice(
                        user_func_wrapped_by_chalice,
                        *self.lumigo_conf_args,
                        **self.lumigo_conf_kwargs,
                    )

                return wrapper2

            return get_decorator
        return original_attr

    def __call__(self, *args, **kwargs):
        if len(args) < 2 and "context" not in kwargs:
            kwargs["context"] = getattr(getattr(self.app, "current_request", None), "context", None)
        return self.lumigo_app(*args, **kwargs)


def wrap_http_calls():
    global already_wrapped
    if not already_wrapped:
        with lumigo_safe_execute("wrap http calls"):
            get_logger().debug("wrapping the http request")
            wrap_function_wrapper("http.client", "HTTPConnection.send", _request_wrapper)
            wrap_function_wrapper(
                "http.client", "HTTPConnection.request", _headers_reminder_wrapper
            )
            wrap_function_wrapper("botocore.awsrequest", "AWSRequest.__init__", _putheader_wrapper)
            wrap_function_wrapper("http.client", "HTTPConnection.getresponse", _response_wrapper)
            wrap_function_wrapper("http.client", "HTTPResponse.read", _read_wrapper)
            if importlib.util.find_spec("urllib3"):
                wrap_function_wrapper(
                    "urllib3.response", "HTTPResponse.read_chunked", _read_stream_wrapper
                )
            already_wrapped = True
