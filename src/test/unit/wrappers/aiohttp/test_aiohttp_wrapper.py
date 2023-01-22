import asyncio
import json

import aiohttp
import pytest

import lumigo_tracer
from lumigo_tracer.lambda_tracer.spans_container import SpansContainer


def test_aiohttp_happy_flow(context, token):
    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        async def main():
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "http://www.google.com/path", data=b"123", headers={"test": "header"}
                ) as response:
                    return await response.text()

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(main())

    result = lambda_test_function({}, context)
    assert result
    http_spans = list(SpansContainer.get_span().spans.values())
    assert http_spans
    assert "started" in http_spans[0]
    assert "ended" in http_spans[0]
    http_info = http_spans[0]["info"]["httpInfo"]
    assert http_info["host"] == "www.google.com"
    # request
    assert json.loads(http_spans[0]["info"]["httpInfo"]["request"]["headers"]) == {"test": "header"}
    assert http_info["request"]["body"] == json.dumps("123")
    assert http_info["request"]["method"] == "POST"
    assert http_info["request"]["uri"] == "http://www.google.com/path"
    # response
    assert (
        json.loads(http_info["response"]["headers"])["content-type"] == "text/html; charset=UTF-8"
    )
    assert http_info["response"]["statusCode"] == 404
    assert http_info["response"]["body"].startswith('"<!DOCTYPE html')


def test_aiohttp_exception(context, token):
    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        async def main():
            async with aiohttp.ClientSession() as session:
                async with session.post("other://www.google.com") as response:
                    return await response.text()

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(main())

    with pytest.raises(Exception):
        lambda_test_function({}, context)

    http_spans = list(SpansContainer.get_span().spans.values())
    assert http_spans
    assert http_spans[0].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "started" in http_spans[0]
    assert http_spans[0]["error"]


def test_aiohttp_session_init_wrapper_misused_traces(context, token):
    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        async def main():
            async with aiohttp.ClientSession(trace_configs="123"):
                pass

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(main())

    with pytest.raises(Exception):
        lambda_test_function({}, context)

    assert not SpansContainer.get_span().spans
    function_span = SpansContainer.get_span().function_span
    assert function_span.get("error", {}).get("type") == "AttributeError"
