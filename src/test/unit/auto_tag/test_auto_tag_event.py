import datetime

import pytest

from lumigo_tracer.auto_tag import auto_tag_event
from lumigo_tracer.auto_tag.auto_tag_event import (
    AutoTagEvent,
    ConfigurationHandler,
    EventAutoTagHandler,
)
from lumigo_tracer.lambda_tracer.spans_container import SpansContainer
from lumigo_tracer.lumigo_utils import Configuration


class ExceptionHandler(EventAutoTagHandler):
    @staticmethod
    def is_supported(event) -> bool:
        raise Exception()

    @staticmethod
    def auto_tag(event):
        raise Exception()


def test_auto_tag_event_is_none():
    AutoTagEvent.auto_tag_event(event=None)

    assert SpansContainer.get_span().execution_tags == []


def test_auto_tag_exception():
    event = {"a": 1}

    AutoTagEvent.auto_tag_event(event=event, handlers=[ExceptionHandler()])

    assert SpansContainer.get_span().execution_tags == []


def test_auto_tag_key_not_in_header(monkeypatch):
    set_header_key(monkeypatch, "not-exists")

    event = {
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
            "requestTimeEpoch": 1587279976628,
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

    AutoTagEvent.auto_tag_event(event=event)

    assert SpansContainer.get_span().execution_tags == []


def test_auto_tag_key_in_header(monkeypatch, lambda_traced):
    set_header_key(monkeypatch, "Accept")

    event = {
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
            "requestTimeEpoch": 1587279976628,
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

    AutoTagEvent.auto_tag_event(event=event)

    assert SpansContainer.get_span().execution_tags == [
        {"key": "Accept", "value": "application/json, text/plain, */*"}
    ]


def set_header_key(monkeypatch, header: str):
    monkeypatch.setattr(auto_tag_event, "AUTO_TAG_API_GW_HEADERS", [header])


@pytest.mark.parametrize(
    "config, event, expected",
    [(["key1"], {"key1": "value1"}, True), (["key1"], {"key2": "value2"}, False), ([], {}, False)],
)
def test_configuration_handler_is_supported(config, event, expected):
    Configuration.auto_tag = config
    assert ConfigurationHandler.is_supported(event) == expected


@pytest.mark.parametrize(
    "auto_tag_keys, event, result_tags",
    [
        (  # happy flow non-nested
            ["key1", "key2", "key3"],
            {"key1": "value1", "key2": "value2", "other": "other"},
            [{"key": "key1", "value": "value1"}, {"key": "key2", "value": "value2"}],
        ),
        (["key1.key2"], {"key1": "value1"}, []),  # not exists inner key
        (["key1.key2"], {"other": "other"}, []),  # not exists outer key
        (  # happy flow nested
            ["key1.key2"],
            {"key1": {"key2": "value"}, "key3": "other"},
            [{"key": "key1.key2", "value": "value"}],
        ),
        (  # happy flow nested multiple keys
            ["key1.key2", "key3.key4"],
            {"key1": {"key2": "value"}, "key3": {"key4": "value2"}, "key5": "other"},
            [{"key": "key1.key2", "value": "value"}, {"key": "key3.key4", "value": "value2"}],
        ),
    ],
)
def test_configuration_handler_auto_tag(auto_tag_keys, event, result_tags, lambda_traced):
    Configuration.auto_tag = auto_tag_keys
    ConfigurationHandler.auto_tag(event)
    tags = SpansContainer.get_span().execution_tags
    assert len(tags) == len(result_tags)
    for tag in result_tags:
        assert tag in tags


def test_configuration_handler_auto_tag_failure(capsys, lambda_traced):
    Configuration.auto_tag = [None, "key2"]
    ConfigurationHandler.auto_tag({"key1": datetime, "key2": "value"})
    tags = SpansContainer.get_span().execution_tags
    assert tags == [{"key": "key2", "value": "value"}]
    assert "Failed to auto tag" in capsys.readouterr().out


@pytest.mark.parametrize(
    "value, expected", [(1, "1"), ({"a": "b"}, "{'a': 'b'}")],  # int  # dict
)
def test_configuration_handler_auto_tag_non_string(value, expected, lambda_traced):
    Configuration.auto_tag = ["key1"]

    ConfigurationHandler.auto_tag({"key1": value})

    tags = SpansContainer.get_span().execution_tags
    assert {"key": "key1", "value": expected} in tags
