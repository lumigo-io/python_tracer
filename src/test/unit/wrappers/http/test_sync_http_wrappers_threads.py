import urllib.request
import concurrent.futures
import json

import lumigo_tracer
from lumigo_tracer.spans_container import SpansContainer

COUNT = 5


def test_lambda_with_threads(context, token):
    def to_exec(index):
        urllib.request.urlopen(f"http://postman-echo.com/get?my_index={index}").read()

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
