import copy
import http.client
import json
import socket
import time
import urllib
from io import BytesIO
from types import SimpleNamespace
from typing import Dict

import boto3
import pytest
import requests
import urllib3
from lumigo_core.configuration import DEFAULT_MAX_ENTRY_SIZE, CoreConfiguration
from urllib3 import HTTPConnectionPool

import lumigo_tracer
from lumigo_tracer.auto_tag import auto_tag_event
from lumigo_tracer.lambda_tracer.spans_container import SpansContainer
from lumigo_tracer.lumigo_utils import TRUNCATE_SUFFIX, Configuration
from lumigo_tracer.wrappers.http.http_data_classes import HttpRequest
from lumigo_tracer.wrappers.http.http_parser import Parser
from lumigo_tracer.wrappers.http.sync_http_wrappers import (
    _putheader_wrapper,
    add_request_event,
    is_lumigo_edge,
    update_event_response,
)


def test_lambda_wrapper_http(context, token):
    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        time.sleep(0.01)
        http.client.HTTPConnection("www.google.com").request("POST", "/")

    lambda_test_function({}, context)
    http_spans = list(SpansContainer.get_span().spans.values())
    assert http_spans
    assert http_spans[0].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "started" in http_spans[0]
    assert http_spans[0]["started"] > SpansContainer.get_span().function_span["started"]
    assert "ended" in http_spans[0]
    assert "content-length" in http_spans[0]["info"]["httpInfo"]["request"]["headers"]
    assert http_spans[0]["info"]["httpInfo"]["request"].get("instance_id") is not None


def test_lambda_wrapper_query_with_http_params(context, token):
    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        http.client.HTTPConnection("www.google.com").request("GET", "/?q=123")

    lambda_test_function({}, context)
    http_spans = list(SpansContainer.get_span().spans.values())

    assert http_spans
    assert http_spans[0]["info"]["httpInfo"]["request"]["uri"] == "www.google.com/?q=123"
    assert http_spans[0]["info"]["httpInfo"]["request"].get("instance_id") is not None


def test_uri_requests(context, token):
    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("POST", "/?q=123", b"123")
        conn.send(BytesIO(b"456"))

    lambda_test_function({}, context)
    http_spans = list(SpansContainer.get_span().spans.values())

    assert http_spans
    assert http_spans[0]["info"]["httpInfo"]["request"]["uri"] == "www.google.com/?q=123"
    assert http_spans[0]["info"]["httpInfo"]["request"].get("instance_id") is not None


def test_lambda_wrapper_get_response(context, token):
    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("GET", "")
        conn.getresponse()

    lambda_test_function({}, context)
    http_spans = list(SpansContainer.get_span().spans.values())

    assert http_spans
    assert http_spans[0]["info"]["httpInfo"]["response"]["statusCode"] == 200
    assert http_spans[0]["info"]["httpInfo"]["request"].get("instance_id") is not None


def test_lambda_wrapper_http_splitted_send(context, token):
    """
    This is a test for the specific case of requests, where they split the http requests into headers and body.
    We didn't use directly the package requests in order to keep the dependencies small.
    """

    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("POST", "/", b"123")
        conn.send(BytesIO(b"456"))

    lambda_test_function({}, context)
    http_spans = list(SpansContainer.get_span().spans.values())
    assert http_spans
    assert http_spans[0]["info"]["httpInfo"]["request"]["body"] == '"123456"'
    assert "content-length" in http_spans[0]["info"]["httpInfo"]["request"]["headers"]
    assert http_spans[0]["info"]["httpInfo"]["request"].get("instance_id") is not None


def test_lambda_wrapper_no_headers(context, token):
    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        http.client.HTTPConnection("www.google.com").send(BytesIO(b"123"))

    lambda_test_function({}, context)
    http_events = list(SpansContainer.get_span().spans.values())
    assert len(http_events) == 1
    assert http_events[0].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "started" in http_events[0]
    assert "ended" in http_events[0]


def test_lambda_wrapper_http_non_splitted_send(context, token):
    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        http.client.HTTPConnection("www.google.com").request("POST", "/")
        http.client.HTTPConnection("www.github.com").send(BytesIO(b"123"))

    lambda_test_function({}, context)
    http_events = list(SpansContainer.get_span().spans.values())
    assert len(http_events) == 2


def test_lambda_wrapper_http_same_connection_two_requests(context, token):
    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        a = http.client.HTTPConnection("www.google.com")
        a.request("POST", "/")
        a.getresponse()
        a.request("GET", "/")

    lambda_test_function({}, context)
    http_events = list(SpansContainer.get_span().spans.values())
    assert len(http_events) == 2


def test_catch_file_like_object_sent_on_http(context, token):
    class A:
        done = False

        def seek(self, where):
            pass

        def tell(self):
            return 1

        def read(self, amount=None):
            if self.done:
                return None
            self.done = True
            return b"body"

    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        try:
            http.client.HTTPConnection("www.github.com").send(A())
        except Exception:
            # We don't care about errors
            pass

    lambda_test_function({}, context)
    http_events = list(SpansContainer.get_span().spans.values())
    assert len(http_events) == 1
    span = list(SpansContainer.get_span().spans.values())[0]
    assert span["info"]["httpInfo"]["request"]["body"] == '"body"'
    assert span["info"]["httpInfo"]["request"].get("instance_id") is not None


def test_bad_domains_scrubber(monkeypatch, context, token):
    monkeypatch.setenv("LUMIGO_DOMAINS_SCRUBBER", '["bad json')

    @lumigo_tracer.lumigo_tracer(token=token, should_report=True)
    def lambda_test_function(event, context):
        pass

    lambda_test_function({}, context)
    assert CoreConfiguration.should_report is False


def test_domains_scrubber_happy_flow(monkeypatch, context, token):
    @lumigo_tracer.lumigo_tracer(token=token, domains_scrubber=[".*google.*"])
    def lambda_test_function(event, context):
        return http.client.HTTPConnection(host="www.google.com").send(b"\r\n")

    lambda_test_function({}, context)
    http_events = list(SpansContainer.get_span().spans.values())
    assert len(http_events) == 1
    assert http_events[0].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
    assert "headers" not in http_events[0]["info"]["httpInfo"]["request"]
    assert http_events[0]["info"]["httpInfo"]["request"]["body"] == "The data is not available"
    assert http_events[0]["info"]["httpInfo"]["request"].get("instance_id") is not None


def test_domains_scrubber_override_allows_default_domains(monkeypatch, context, token):
    ssm_url = "www.ssm.123.amazonaws.com"

    @lumigo_tracer.lumigo_tracer(token=token, domains_scrubber=[".*google.*"])
    def lambda_test_function(event, context):
        try:
            return http.client.HTTPConnection(host=ssm_url).send(b"\r\n")
        except Exception:
            return

    lambda_test_function({}, context)
    http_events = list(SpansContainer.get_span().spans.values())
    assert len(http_events) == 1
    assert http_events[0].get("info", {}).get("httpInfo", {}).get("host") == ssm_url
    assert http_events[0]["info"]["httpInfo"]["request"]["headers"]
    assert http_events[0]["info"]["httpInfo"]["request"].get("instance_id") is not None


def test_wrapping_json_request(context, token):
    @lumigo_tracer.lumigo_tracer()
    def lambda_test_function(event, context):
        urllib.request.urlopen(
            urllib.request.Request(
                "http://api.github.com", b"{}", headers={"Content-Type": "application/json"}
            )
        )
        return 1

    assert lambda_test_function({}, context) == 1
    http_events = list(SpansContainer.get_span().spans.values())
    assert any(
        '"content-type": "application/json"'
        in event.get("info", {}).get("httpInfo", {}).get("request", {}).get("headers", "")
        for event in http_events
    )


def test_exception_in_parsers(monkeypatch, caplog, context, token):
    monkeypatch.setattr(Parser, "parse_request", Exception)

    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        return http.client.HTTPConnection(host="www.google.com").send(b"\r\n")

    lambda_test_function({}, context)
    assert caplog.records[-1].msg == "An exception occurred in lumigo's code add request event"


def test_wrapping_urlib_stream_get(context, token):
    """
    This is the same case as the one of `requests.get`.
    """

    @lumigo_tracer.lumigo_tracer()
    def lambda_test_function(event, context):
        r = urllib3.PoolManager().urlopen("GET", "https://www.google.com", preload_content=False)
        return b"".join(r.stream(32))

    lambda_test_function({}, context)
    assert len(SpansContainer.get_span().spans) == 1
    event = list(SpansContainer.get_span().spans.values())[0]
    assert event["info"]["httpInfo"]["response"]["body"]
    assert event["info"]["httpInfo"]["response"]["statusCode"] == 200
    assert event["info"]["httpInfo"]["host"] == "www.google.com"
    assert event["info"]["httpInfo"]["request"].get("instance_id") is not None


def test_wrapping_requests_times(monkeypatch, context, token):
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
    span = list(SpansContainer.get_span().spans.values())[0]
    assert span["started"] - start_time < 100


@pytest.mark.parametrize(
    "func_to_patch",
    [
        (socket, "getaddrinfo"),  # this function is being called before the request
        (http.client.HTTPConnection, "getresponse"),  # after the request
    ],
)
def test_requests_failure_before_http_call(monkeypatch, context, func_to_patch, token):
    monkeypatch.setenv("LUMIGO_SYNC_TRACING", "true")

    @lumigo_tracer.lumigo_tracer()
    def lambda_test_function(event, context):
        try:
            requests.post("https://www.google.com", data=b"123", headers={"a": "b"})
        except ZeroDivisionError:
            return True
        return False

    # requests executes this function before/after the http call
    monkeypatch.setattr(*func_to_patch, lambda *args, **kwargs: 1 / 0)

    assert lambda_test_function({}, context) is True

    assert len(SpansContainer.get_span().spans) == 1
    span = list(SpansContainer.get_span().spans.values())[0]
    assert span["error"]["message"] == "division by zero"
    assert span["info"]["httpInfo"]["request"]["method"] == "POST"
    assert span["info"]["httpInfo"]["request"]["body"] == '"123"'
    assert span["info"]["httpInfo"]["request"]["headers"]
    assert span["info"]["httpInfo"]["request"].get("instance_id") is not None


def test_requests_failure_with_kwargs(monkeypatch, context, token):
    monkeypatch.setenv("LUMIGO_SYNC_TRACING", "true")

    @lumigo_tracer.lumigo_tracer()
    def lambda_test_function(event, context):
        try:
            requests.request(
                method="POST", url="https://www.google.com", data=b"123", headers={"a": "b"}
            )
        except ZeroDivisionError:
            return True
        return False

    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: 1 / 0)

    assert lambda_test_function({}, context) is True

    assert len(SpansContainer.get_span().spans) == 1
    span = list(SpansContainer.get_span().spans.values())[0]
    assert span["info"]["httpInfo"]["request"]["method"] == "POST"


def test_wrapping_with_tags_for_api_gw_headers(monkeypatch, context, token, lambda_traced):
    monkeypatch.setattr(auto_tag_event, "AUTO_TAG_API_GW_HEADERS", ["Accept"])

    @lumigo_tracer.lumigo_tracer()
    def lambda_test_function(event, context):
        return "ret_value"

    result = lambda_test_function(api_gw_event(), context)

    assert result == "ret_value"
    assert SpansContainer.get_span().execution_tags == [
        {"key": "Accept", "value": "application/json, text/plain, */*"}
    ]


def test_correct_headers_of_send_after_request(context, token):
    @lumigo_tracer.lumigo_tracer()
    def lambda_test_function(event, context):
        d = {"a": "b", "myPassword": "123"}
        conn = http.client.HTTPConnection("www.google.com")
        conn.request("POST", "/", json.dumps(d), headers={"a": b"b"})
        conn.send(b"GET\r\nc: d\r\n\r\nbody")
        return {"lumigo": "rulz"}

    lambda_test_function({"key": "24"}, context)
    spans = list(SpansContainer.get_span().spans.values())
    assert spans[0]["info"]["httpInfo"]["request"]["headers"] == json.dumps({"a": "b"})
    assert spans[1]["info"]["httpInfo"]["request"]["headers"] == json.dumps({"c": "d"})
    assert spans[0]["info"]["httpInfo"]["request"].get("instance_id") is not None
    assert spans[1]["info"]["httpInfo"]["request"].get("instance_id") is not None


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
def test_lumigo_doesnt_change_event(given_event, token):
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
    span = add_request_event(
        None,
        HttpRequest(
            host="dummy", method="dummy", uri="dummy", headers={"dummy": "dummy"}, body="dummy"
        ),
    )

    big_response_chunk = b'leak"' * DEFAULT_MAX_ENTRY_SIZE
    for _ in range(10):
        update_event_response(
            span["id"], host=None, status_code=200, headers=None, body=big_response_chunk
        )
    body = list(SpansContainer.get_span().spans.values())[0]["info"]["httpInfo"]["response"]["body"]
    assert len(body) <= len(big_response_chunk)
    assert body[: -len(TRUNCATE_SUFFIX)] in json.dumps(big_response_chunk.decode())


def test_double_response_size_limit_on_error_status_code(context, monkeypatch, token):
    d = {"a": "v" * int(CoreConfiguration.get_max_entry_size() * 1.5)}
    original_begin = http.client.HTTPResponse.begin

    def mocked_begin(*args, **kwargs):
        """
        We need this in order to mock the status code of the response
        """
        return_value = original_begin(*args, **kwargs)
        response_self = args[0]
        response_self.code = status_code
        response_self.headers = {"a": "v" * int(CoreConfiguration.get_max_entry_size() * 1.5)}
        return return_value

    monkeypatch.setattr(http.client.HTTPResponse, "begin", mocked_begin)

    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        conn = http.client.HTTPSConnection("httpbin.org")
        conn.request("GET", "/get", json.dumps(d), headers=d)
        conn.getresponse()

    status_code = 200
    lambda_test_function({}, context)
    http_span_no_error = copy.deepcopy(list(SpansContainer.get_span().spans.values())[-1])

    status_code = 400
    lambda_test_function({}, context)
    http_span_with_error = list(SpansContainer.get_span().spans.values())[-1]

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


def test_on_error_status_code_not_scrub_dynamodb(context, monkeypatch, token):
    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        try:
            table = boto3.resource("dynamodb").Table("not-exist")
            table.get_item(Key={"field0": "v"})
        except Exception:
            pass

    lambda_test_function({}, context)
    http_info = list(SpansContainer.get_span().spans.values())[-1]["info"]["httpInfo"]

    assert http_info["response"]["statusCode"] >= 400  # Verify error occurred
    # Verify `Key` wasn't scrubbed
    assert "field0" in json.loads(http_info["request"]["body"])["Key"]


@pytest.mark.parametrize("host, is_lumigo", [("https://lumigo.io", True), ("google.com", False)])
def test_is_lumigo_edge(host, is_lumigo, monkeypatch):
    monkeypatch.setattr(Configuration, "host", "lumigo.io")
    assert is_lumigo_edge(host) == is_lumigo


def test_same_connection_id_for_same_connection(context, token):
    @lumigo_tracer.lumigo_tracer(token=token)
    def lambda_test_function(event, context):
        pool = HTTPConnectionPool("www.google.com", maxsize=1)
        pool.request("GET", "/?q=123")
        pool.request("GET", "/?q=1234")

    lambda_test_function({}, context)
    http_spans = list(SpansContainer.get_span().spans.values())

    instance_id_1 = http_spans[0]["info"]["httpInfo"]["request"].get("instance_id")
    instance_id_2 = http_spans[1]["info"]["httpInfo"]["request"].get("instance_id")
    assert instance_id_1 == instance_id_2


def test_wrapping_boto3_core_aws_request(monkeypatch):
    monkeypatch.setattr(SpansContainer, "can_path_root", lambda *args, **kwargs: True)
    monkeypatch.setattr(SpansContainer, "get_patched_root", lambda *args, **kwargs: "123")

    def func(arg1, arg2, kwarg3=None, kwarg4=None, headers=None):
        assert arg1 == 1
        assert arg2 == 2
        assert kwarg3 == 3
        assert kwarg4 == 4
        assert headers == {"X-Amzn-Trace-Id": "123"}
        return 5

    response = _putheader_wrapper(
        func=func, instance=None, args=(1, 2), kwargs={"kwarg3": 3, "kwarg4": 4, "headers": {}}
    )

    assert response == 5


def test_wrapping_boto3_core_aws_request_fail_safe(monkeypatch):
    monkeypatch.setattr(SpansContainer, "can_path_root", lambda *args, **kwargs: True)

    def mock_get_patched_root(*args, **kwargs):
        raise Exception("mock exception")

    monkeypatch.setattr(SpansContainer, "get_patched_root", mock_get_patched_root)

    def func(arg1, arg2, kwarg3=None, kwarg4=None, headers=None):
        assert arg1 == 1
        assert arg2 == 2
        assert kwarg3 == 3
        assert kwarg4 == 4
        assert headers == {"original": "header"}
        return 5

    response = _putheader_wrapper(
        func=func,
        instance=None,
        args=(1, 2),
        kwargs={"kwarg3": 3, "kwarg4": 4, "headers": {"original": "header"}},
    )

    assert response == 5


def test_wrapping_boto3_core_aws_request_no_headers_kwargs(monkeypatch):
    monkeypatch.setattr(SpansContainer, "can_path_root", lambda *args, **kwargs: True)
    monkeypatch.setattr(SpansContainer, "get_patched_root", lambda *args, **kwargs: "123")

    def func(arg1, arg2, kwarg3=None, kwarg4=None, headers=None):
        assert arg1 == 1
        assert arg2 == 2
        assert kwarg3 == 3
        assert kwarg4 == 4
        assert headers is None
        return 5

    response = _putheader_wrapper(
        func=func, instance=None, args=(1, 2), kwargs={"kwarg3": 3, "kwarg4": 4},
    )

    assert response == 5
