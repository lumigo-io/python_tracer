from lumigo_tracer.libs.wrapt import wrap_function_wrapper
import http.client
from io import BytesIO
from lumigo_tracer.span import Span, EventType


_BODY_HEADER_SPLITTER = b"\r\n\r\n"
_FLAGS_HEADER_SPLITTER = b"\r\n"


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
            Span.get_span().add_event(url, headers, body, EventType.REQUEST)
            return func(*args, **kwargs)

    Span.get_span().add_event(None, None, args[0], EventType.REQUEST)
    return func(*args, **kwargs)


def _response_wrapper(func, instance, args, kwargs):
    """
    This is the wrapper of the function that called after that the http request was sent.
    Note that we don't examine the response data because it may change the original behaviour (ret_val.peek()).
    """
    ret_val = func(*args, **kwargs)
    headers = ret_val.headers
    Span.get_span().add_event(instance.host, headers, "", EventType.RESPONSE)
    return ret_val


def _read_wrapper(func, instance, args, kwargs):
    """
    This is the wrapper to he function that reads the response data, and update the previous event.
    """
    ret_val = func(*args, **kwargs)
    Span.get_span().update_event(instance.headers, ret_val)
    return ret_val


def lumigo_lambda(func):
    """
    This function should be used as wrapper to your lambda function.
    It will trace your HTTP calls and send it to our backend, which will help you understand it better.
    """

    def lambda_wrapper(*args, **kwargs):
        Span.create_span(args[1] if args and len(args) > 1 else None)
        try:
            ret_val = func(*args, **kwargs)
        except Exception as e:
            Span.get_span().add_exception_event(e)
            raise
        finally:
            Span.get_span().end()
        return ret_val

    return lambda_wrapper


wrap_function_wrapper("http.client", "HTTPConnection.send", _request_wrapper)
wrap_function_wrapper("http.client", "HTTPConnection.getresponse", _response_wrapper)
wrap_function_wrapper("http.client", "HTTPResponse.read", _read_wrapper)
