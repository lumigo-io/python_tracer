from lumigo_tracer.libs.wrapt import wrap_function_wrapper
from aiohttp.client import URL
from lumigo_tracer.span import Span, EventType


async def request_wrapper(func, instance, args, kwargs):
    body = kwargs.get("data", "")
    if isinstance(body, str):
        body = body.encode()
    url = URL(args[1])
    headers = {k: v for k, v in kwargs.get("headers", {}).items()}
    Span.get_span().add_event(url.host, headers, body, EventType.REQUEST)
    ret_val = await func(*args, **kwargs)

    # if should_wait:
    #     # What should we do if they never awaited to an answer?
    #     host = ret_val.real_url.host
    #     body = await ret_val.text()
    #     span.add_event(f'I see response {host}: {body[:70]}')

    return ret_val


async def response_wrapper(func, instance, args, kwargs):
    body = await func(*args, **kwargs)
    url = instance.url.host
    headers = {k: v for k, v in instance.raw_headers}
    Span.get_span().add_event(url, headers, body, EventType.RESPONSE)
    return body


def lumigo_async_lambda(func):
    """
    This function should be used as wrapper to your lambda function.
    It will trace your HTTP calls and send it to our backend, which will help you understand it better.
    """

    async def lambda_wrapper(*args, **kwargs):
        Span.create_span(func.__name__)
        ret_val = await func(*args, **kwargs)
        Span.get_span().end()
        return ret_val

    return lambda_wrapper


wrap_function_wrapper("aiohttp.client", "ClientSession._request", request_wrapper)
wrap_function_wrapper("aiohttp.client", "ClientResponse.read", response_wrapper)
