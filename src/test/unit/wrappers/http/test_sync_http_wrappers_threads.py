import asyncio
import concurrent.futures
import json
import urllib.request

import requests

import lumigo_tracer
from lumigo_tracer.lambda_tracer.spans_container import SpansContainer

COUNT = 5


def test_lambda_with_threads(context, token):
    def to_exec(index):
        req = urllib.request.Request(
            f"https://postman-echo.com/get?my_index={index}",
            headers={"User-Agent": "LumigoTracerTest/1.0"},
        )
        urllib.request.urlopen(req).read()

    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        with concurrent.futures.ThreadPoolExecutor(max_workers=COUNT) as executor:
            futures = [executor.submit(to_exec, index) for index in range(COUNT)]
            [f.result() for f in futures]

    lambda_test_function({}, context)
    http_spans = list(SpansContainer.get_span().spans.values())

    assert len(http_spans) == COUNT
    for span in http_spans:
        request_index = span["info"]["httpInfo"]["request"]["uri"][-1]
        print(json.loads(span["info"]["httpInfo"]["response"]["body"]))
        response_index = json.loads(span["info"]["httpInfo"]["response"]["body"])["args"][
            "my_index"
        ]
        assert request_index == response_index


def test_run_in_executor(context, token):
    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        async def main():
            loop = asyncio.get_event_loop()
            future1 = loop.run_in_executor(None, requests.get, "http://www.google.com")
            future2 = loop.run_in_executor(None, requests.get, "http://www.google.com")
            responses = await asyncio.gather(future1, future2)
            return responses[0].text

        loop = asyncio.get_event_loop()
        return loop.run_until_complete(main())

    result = lambda_test_function({}, context)
    assert result
    http_spans = list(SpansContainer.get_span().spans.values())
    assert http_spans
    assert http_spans[0].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "started" in http_spans[0]
    assert "ended" in http_spans[0]
    assert "user-agent" in http_spans[0]["info"]["httpInfo"]["request"]["headers"]
