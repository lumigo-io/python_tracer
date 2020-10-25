import copy
import json
import time
import http.client
import urllib
from io import BytesIO
from types import SimpleNamespace
from typing import Dict
import socket

import pytest
import urllib3
import requests

import lumigo_tracer
from lumigo_tracer import lumigo_utils
from lumigo_tracer.auto_tag import auto_tag_event
from lumigo_tracer.lumigo_utils import EXECUTION_TAGS_KEY, DEFAULT_MAX_ENTRY_SIZE, Configuration
from lumigo_tracer.wrappers.http.http_parser import Parser
from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.wrappers.http.http_data_classes import HttpState, HttpRequest
from lumigo_tracer.wrappers.http.sync_http_wrappers import add_request_event, update_event_response


def test_lambda_wrapper_http(context):
    @lumigo_tracer.lumigo_tracer(token="123")
    def lambda_test_function(event, context):
        time.sleep(0.01)
        http.client.HTTPConnection("www.google.com").request("POST", "/")

    lambda_test_function({}, context)
    http_spans = SpansContainer.get_span().spans
    assert http_spans
    assert http_spans[0].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "started" in http_spans[0]
    assert http_spans[0]["started"] > SpansContainer.get_span().function_span["started"]
    assert "ended" in http_spans[0]
    assert "content-length" in http_spans[0]["info"]["httpInfo"]["request"]["headers"]


def test_lambda_wrapper_query_with_http_params(context):
    @lumigo_tracer.lumigo_tracer(token="123")
    def lambda_test_function(event, context):
        http.client.HTTPConnection("www.google.com").request("GET", "/?q=123")

    lambda_test_function({}, context)
    http_spans = SpansContainer.get_span().spans

    assert http_spans
    assert http_spans[0]["info"]["httpInfo"]["request"]["uri"] == "www.google.com/?q=123"


def test_uri_requests(context):
    @lumigo_tracer.lumigo_tracer(token="123")
    def lambda_test_function(event, context):
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("POST", "/?q=123", b"123")
        conn.send(BytesIO(b"456"))

    lambda_test_function({}, context)
    http_spans = SpansContainer.get_span().spans

    assert http_spans
    assert http_spans[0]["info"]["httpInfo"]["request"]["uri"] == "www.google.com/?q=123"


def test_lambda_wrapper_get_response(context):
    @lumigo_tracer.lumigo_tracer(token="123")
    def lambda_test_function(event, context):
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("GET", "")
        conn.getresponse()

    lambda_test_function({}, context)
    http_spans = SpansContainer.get_span().spans

    assert http_spans
    assert http_spans[0]["info"]["httpInfo"]["response"]["statusCode"] == 200


def test_lambda_wrapper_http_splitted_send(context):
    """
    This is a test for the specific case of requests, where they split the http requests into headers and body.
    We didn't use directly the package requests in order to keep the dependencies small.
    """

    @lumigo_tracer.lumigo_tracer(token="123")
    def lambda_test_function(event, context):
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("POST", "/", b"123")
        conn.send(BytesIO(b"456"))

    lambda_test_function({}, context)
    http_spans = SpansContainer.get_span().spans
    assert http_spans
    assert http_spans[0]["info"]["httpInfo"]["request"]["body"] == '"123456"'
    assert "content-length" in http_spans[0]["info"]["httpInfo"]["request"]["headers"]


def test_lambda_wrapper_no_headers(context):
    @lumigo_tracer.lumigo_tracer(token="123")
    def lambda_test_function(event, context):
        http.client.HTTPConnection("www.google.com").send(BytesIO(b"123"))

    lambda_test_function({}, context)
    http_events = SpansContainer.get_span().spans
    assert len(http_events) == 1
    assert http_events[0].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "started" in http_events[0]
    assert "ended" in http_events[0]


def test_lambda_wrapper_http_non_splitted_send(context):
    @lumigo_tracer.lumigo_tracer(token="123")
    def lambda_test_function(event, context):
        http.client.HTTPConnection("www.google.com").request("POST", "/")
        http.client.HTTPConnection("www.github.com").send(BytesIO(b"123"))

    lambda_test_function({}, context)
    http_events = SpansContainer.get_span().spans
    assert len(http_events) == 2


def test_catch_file_like_object_sent_on_http(context):
    class A:
        def seek(self, where):
            pass

        def tell(self):
            return 1

        def read(self, amount=None):
            return b"body"

    @lumigo_tracer.lumigo_tracer(token="123")
    def lambda_test_function(event, context):
        try:
            http.client.HTTPConnection("www.github.com").send(A())
        except Exception:
            # We don't care about errors
            pass

    lambda_test_function({}, context)
    http_events = SpansContainer.get_span().spans
    assert len(http_events) == 1
    span = SpansContainer.get_span().spans[0]
    assert span["info"]["httpInfo"]["request"]["body"] == '"body"'


def test_bad_domains_scrubber(monkeypatch, context):
    monkeypatch.setenv("LUMIGO_DOMAINS_SCRUBBER", '["bad json')

    @lumigo_tracer.lumigo_tracer(token="123", should_report=True)
    def lambda_test_function(event, context):
        pass

    lambda_test_function({}, context)
    assert lumigo_utils.Configuration.should_report is False


def test_domains_scrubber_happy_flow(monkeypatch, context):
    @lumigo_tracer.lumigo_tracer(token="123", domains_scrubber=[".*google.*"])
    def lambda_test_function(event, context):
        return http.client.HTTPConnection(host="www.google.com").send(b"\r\n")

    lambda_test_function({}, context)
    http_events = SpansContainer.get_span().spans
    assert len(http_events) == 1
    assert http_events[0].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "headers" not in http_events[0]["info"]["httpInfo"]["request"]
    assert http_events[0]["info"]["httpInfo"]["request"]["body"] == "The data is not available"


def test_domains_scrubber_override_allows_default_domains(monkeypatch, context):
    ssm_url = "www.ssm.123.amazonaws.com"

    @lumigo_tracer.lumigo_tracer(token="123", domains_scrubber=[".*google.*"])
    def lambda_test_function(event, context):
        try:
            return http.client.HTTPConnection(host=ssm_url).send(b"\r\n")
        except Exception:
            return

    lambda_test_function({}, context)
    http_events = SpansContainer.get_span().spans
    assert len(http_events) == 1
    assert http_events[0].get("info", {}).get("httpInfo", {}).get("host") == ssm_url
    assert http_events[0]["info"]["httpInfo"]["request"]["headers"]


def test_wrapping_json_request(context):
    @lumigo_tracer.lumigo_tracer()
    def lambda_test_function(event, context):
        urllib.request.urlopen(
            urllib.request.Request(
                "http://api.github.com", b"{}", headers={"Content-Type": "application/json"}
            )
        )
        return 1

    assert lambda_test_function({}, context) == 1
    http_events = SpansContainer.get_span().spans
    assert any(
        '"content-type": "application/json"'
        in event.get("info", {}).get("httpInfo", {}).get("request", {}).get("headers", "")
        for event in http_events
    )


def test_exception_in_parsers(monkeypatch, caplog, context):
    monkeypatch.setattr(Parser, "parse_request", Exception)

    @lumigo_tracer.lumigo_tracer(token="123")
    def lambda_test_function(event, context):
        return http.client.HTTPConnection(host="www.google.com").send(b"\r\n")

    lambda_test_function({}, context)
    assert caplog.records[-1].msg == "An exception occurred in lumigo's code add request event"


def test_wrapping_urlib_stream_get(context):
    """
    This is the same case as the one of `requests.get`.
    """

    @lumigo_tracer.lumigo_tracer()
    def lambda_test_function(event, context):
        r = urllib3.PoolManager().urlopen("GET", "https://www.google.com", preload_content=False)
        return b"".join(r.stream(32))

    lambda_test_function({}, context)
    assert len(SpansContainer.get_span().spans) == 1
    event = SpansContainer.get_span().spans[0]
    assert event["info"]["httpInfo"]["response"]["body"]
    assert event["info"]["httpInfo"]["response"]["statusCode"] == 200
    assert event["info"]["httpInfo"]["host"] == "www.google.com"


def test_wrapping_requests_times(monkeypatch, context):
    @lumigo_tracer.lumigo_tracer()
    def lambda_test_function(event, context):
        start_time = time.time() * 1000
        requests.get("https://www.google.com")
        return start_time

    # add delay to the connection establishment process
    original_getaddrinfo = socket.getaddrinfo

    def delayed_getaddrinfo(*args, **kwargs):
        time.sleep(0.1)
        return original_getaddrinfo(*args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", delayed_getaddrinfo)

    # validate that the added delay didn't affect the start time
    start_time = lambda_test_function({}, context)
    span = SpansContainer.get_span().spans[0]
    assert span["started"] - start_time < 100


def test_wrapping_with_tags_for_api_gw_headers(monkeypatch, context):
    monkeypatch.setattr(auto_tag_event, "AUTO_TAG_API_GW_HEADERS", ["Accept"])

    @lumigo_tracer.lumigo_tracer()
    def lambda_test_function(event, context):
        return "ret_value"

    result = lambda_test_function(api_gw_event(), context)

    assert result == "ret_value"
    assert SpansContainer.get_span().function_span[EXECUTION_TAGS_KEY] == [
        {"key": "Accept", "value": "application/json, text/plain, */*"}
    ]


def test_correct_headers_of_send_after_request(context):
    @lumigo_tracer.lumigo_tracer()
    def lambda_test_function(event, context):
        d = {"a": "b", "myPassword": "123"}
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("POST", "/", json.dumps(d), headers={"a": b"b"})
        conn.send(b"GET\r\nc: d\r\n\r\nbody")
        return {"lumigo": "rulz"}

    lambda_test_function({"key": "24"}, context)
    spans = SpansContainer.get_span().spans
    assert spans[0]["info"]["httpInfo"]["request"]["headers"] == json.dumps({"a": "b"})
    assert spans[1]["info"]["httpInfo"]["request"]["headers"] == json.dumps({"c": "d"})


def api_gw_event() -> Dict:
    return {
        "resource": "/add-user",
        "path": "/add-user",
        "httpMethod": "POST",
        "headers": {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
            "Authorization": "auth",
            "CloudFront-Forwarded-Proto": "https",
            "CloudFront-Is-Desktop-Viewer": "true",
            "CloudFront-Is-Mobile-Viewer": "false",
            "CloudFront-Is-SmartTV-Viewer": "false",
            "CloudFront-Is-Tablet-Viewer": "false",
            "CloudFront-Viewer-Country": "IL",
            "content-type": "application/json;charset=UTF-8",
            "customer_id": "c_1111",
            "Host": "aaaa.execute-api.us-west-2.amazonaws.com",
            "origin": "https://aaa.io",
            "Referer": "https://aaa.io/users",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36",
            "Via": "2.0 59574f77a7cf2d23d64904db278e5711.cloudfront.net (CloudFront)",
            "X-Amz-Cf-Id": "J4KbOEUrZCnUQSLsDq1PyYXmfpVy8x634huSeBX0HCbscgH-N2AtVA==",
            "X-Amzn-Trace-Id": "Root=1-5e9bf868-1c53a38cfe070266db0bfbd9",
            "X-Forwarded-For": "5.102.206.161, 54.182.243.106",
            "X-Forwarded-Port": "443",
            "X-Forwarded-Proto": "https",
        },
        "multiValueHeaders": {
            "Accept": ["application/json, text/plain, */*"],
            "Accept-Encoding": ["gzip, deflate, br"],
            "Accept-Language": ["he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7"],
            "Authorization": ["auth"],
            "CloudFront-Forwarded-Proto": ["https"],
            "CloudFront-Is-Desktop-Viewer": ["true"],
            "CloudFront-Is-Mobile-Viewer": ["false"],
            "CloudFront-Is-SmartTV-Viewer": ["false"],
            "CloudFront-Is-Tablet-Viewer": ["false"],
            "CloudFront-Viewer-Country": ["IL"],
            "content-type": ["application/json;charset=UTF-8"],
            "customer_id": ["c_1111"],
            "Host": ["a.execute-api.us-west-2.amazonaws.com"],
            "origin": ["https://aaa.io"],
            "Referer": ["https://aaa.io/users"],
            "sec-fetch-dest": ["empty"],
            "sec-fetch-mode": ["cors"],
            "sec-fetch-site": ["cross-site"],
            "User-Agent": [
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36"
            ],
            "Via": ["2.0 59574f77a7cf2d23d64904db278e5711.cloudfront.net (CloudFront)"],
            "X-Amz-Cf-Id": ["J4KbOEUrZCnUQSLsDq1PyYXmfpVy8x634huSeBX0HCbscgH-N2AtVA=="],
            "X-Amzn-Trace-Id": ["Root=1-5e9bf868-1c53a38cfe070266db0bfbd9"],
            "X-Forwarded-For": ["5.102.206.161, 54.182.243.106"],
            "X-Forwarded-Port": ["443"],
            "X-Forwarded-Proto": ["https"],
        },
        "queryStringParameters": "1",
        "multiValueQueryStringParameters": "1",
        "pathParameters": "1",
        "stageVariables": None,
        "requestContext": {
            "resourceId": "ua33sn",
            "authorizer": {
                "claims": {
                    "sub": "a87005bb-3030-4962-bae8-48cd629ba20b",
                    "custom:customer": "c_1111",
                    "iss": "https://cognito-idp.us-west-2.amazonaws.com/us-west-2",
                    "custom:customer-name": "a",
                    "cognito:username": "aa",
                    "aud": "4lidcnek50hi18996gadaop8j0",
                    "event_id": "9fe80735-f265-41d5-a7ca-04b88c2a4a4c",
                    "token_use": "id",
                    "auth_time": "1587038744",
                    "exp": "Sun Apr 19 08:06:14 UTC 2020",
                    "custom:role": "admin",
                    "iat": "Sun Apr 19 07:06:14 UTC 2020",
                    "email": "a@a.com",
                }
            },
            "resourcePath": "/add-user",
            "httpMethod": "POST",
            "extendedRequestId": "LOPAXFcuvHcFUKg=",
            "requestTime": "19/Apr/2020:07:06:16 +0000",
            "path": "/prod/add-user",
            "accountId": "114300393969",
            "protocol": "HTTP/1.1",
            "stage": "prod",
            "domainPrefix": "psqn7b0ev2",
            "requestTimeEpoch": 1_587_279_976_628,
            "requestId": "78542821-ca17-4e83-94ec-96993a9d451d",
            "identity": {
                "cognitoIdentityPoolId": None,
                "accountId": None,
                "cognitoIdentityId": None,
                "caller": None,
                "sourceIp": "5.102.206.161",
                "principalOrgId": None,
                "accessKey": None,
                "cognitoAuthenticationType": None,
                "cognitoAuthenticationProvider": None,
                "userArn": None,
                "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36",
                "user": None,
            },
            "domainName": "psqn7b0ev2.execute-api.us-west-2.amazonaws.com",
            "apiId": "psqn7b0ev2",
        },
        "body": '{"email":"a@a.com"}',
        "isBase64Encoded": False,
    }


@pytest.mark.parametrize(
    ["given_event"], ([{"hello": "world"}], [api_gw_event()])  # happy flow  # apigw event
)
def test_lumigo_doesnt_change_event(given_event):
    origin_event = copy.deepcopy(given_event)

    @lumigo_tracer.lumigo_tracer()
    def lambda_test_function(event, context):
        assert event == origin_event
        return "ret_value"

    lambda_test_function(given_event, SimpleNamespace(aws_request_id="1234"))


def test_aggregating_response_body():
    """
    This test is here to validate that we're not leaking memory on aggregating response body.
    Unfortunately python doesn't give us better tools, so we must check the problematic member itself.
    """
    SpansContainer.create_span()
    add_request_event(
        HttpRequest(
            host="dummy", method="dummy", uri="dummy", headers={"dummy": "dummy"}, body="dummy"
        )
    )

    big_response_chunk = b"leak" * DEFAULT_MAX_ENTRY_SIZE
    for _ in range(10):
        update_event_response(host=None, status_code=200, headers=None, body=big_response_chunk)
    assert len(HttpState.previous_response_body) <= len(big_response_chunk)


def test_double_request_size_limit_on_error_status_code(context, monkeypatch):
    d = {"a": "v" * int(Configuration.get_max_entry_size() * 1.5)}
    original_begin = http.client.HTTPResponse.begin

    def mocked_begin(*args, **kwargs):
        """
        We need this in order to mock the status code of the response
        """
        return_value = original_begin(*args, **kwargs)
        response_self = args[0]
        response_self.code = status_code
        return return_value

    monkeypatch.setattr(http.client.HTTPResponse, "begin", mocked_begin)

    @lumigo_tracer.lumigo_tracer(token="123")
    def lambda_test_function(event, context):
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("GET", "/", json.dumps(d), headers=d)
        conn.getresponse()

    status_code = 200
    lambda_test_function({}, context)
    http_span_no_error = copy.deepcopy(SpansContainer.get_span().spans[-1])

    status_code = 400
    lambda_test_function({}, context)
    http_span_with_error = SpansContainer.get_span().spans[-1]

    http_info_no_error = http_span_no_error["info"]["httpInfo"]
    http_info_with_error = http_span_with_error["info"]["httpInfo"]
    request_with_error = http_info_with_error["request"]
    request_no_error = http_info_no_error["request"]

    assert http_info_no_error["response"]["statusCode"] == 200
    assert http_info_with_error["response"]["statusCode"] >= 400

    assert len(request_with_error["headers"]) > len(request_no_error["headers"])
    assert request_with_error["headers"] == json.dumps(d)

    assert len(request_with_error["body"]) > len(request_no_error["body"])
    assert request_with_error["body"] == json.dumps(d)
