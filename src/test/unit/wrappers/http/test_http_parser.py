import json

import pytest
from lumigo_tracer.lumigo_utils import Configuration
from lumigo_tracer.w3c_context import TRACEPARENT_HEADER_NAME

from lumigo_tracer.wrappers.http.http_data_classes import HttpRequest
from lumigo_tracer.wrappers.http.http_parser import (
    ServerlessAWSParser,
    Parser,
    get_parser,
    ApiGatewayV2Parser,
    DynamoParser,
    EventBridgeParser,
    LambdaParser,
    S3Parser,
    SqsParser,
    SnsParser,
)


def test_serverless_aws_parser_fallback_doesnt_change():
    url = "https://kvpuorrsqb.execute-api.us-west-2.amazonaws.com"
    headers = {"nothing": "relevant"}
    serverless_parser = ServerlessAWSParser().parse_response(url, 200, headers=headers, body=b"")
    root_parser = Parser().parse_response(url, 200, headers=headers, body=b"")
    serverless_parser.pop("ended")
    root_parser.pop("ended")
    assert serverless_parser == root_parser


def test_get_parser_check_headers():
    url = "api.dev.com"
    headers = {"x-amzn-requestid": "1234"}
    assert get_parser(url, headers) == ServerlessAWSParser


def test_get_parser_s3():
    url = "s3.eu-west-1.amazonaws.com"
    headers = {"key": "value"}
    assert get_parser(url, headers) == S3Parser


def test_get_parser_apigw():
    url = "https://ne3kjv28fh.execute-api.us-west-2.amazonaws.com/doriaviram"
    assert get_parser(url, {}) == ApiGatewayV2Parser


def test_get_parser_non_aws():
    url = "events.other.service"
    assert get_parser(url, {}) == Parser


def test_get_default_parser_when_using_extension(monkeypatch):
    monkeypatch.setenv("LUMIGO_USE_TRACER_EXTENSION", "TRUE")
    url = "https://ne3kjv28fh.execute-api.us-west-2.amazonaws.com/doriaviram"
    assert get_parser(url, {}) == Parser


def test_apigw_parse_response():
    parser = ApiGatewayV2Parser()
    headers = {"apigw-requestid": "LY_66j0dPHcESCg="}

    result = parser.parse_response("dummy", 200, headers, body=b"")

    assert result["info"] == {
        "messageId": "LY_66j0dPHcESCg=",
        "httpInfo": {
            "host": "dummy",
            "response": {
                "headers": '{"apigw-requestid": "LY_66j0dPHcESCg="}',
                "body": "",
                "statusCode": 200,
            },
        },
    }


def test_apigw_parse_response_with_aws_request_id():
    parser = ApiGatewayV2Parser()
    headers = {
        "apigw-requestid": "LY_66j0dPHcESCg=",
        "x-amzn-requestid": "x-amzn-requestid_LY_66j0dPHcESCg=",
    }

    result = parser.parse_response("dummy", 200, headers, body=b"")

    assert result["info"] == {
        "messageId": "x-amzn-requestid_LY_66j0dPHcESCg=",
        "httpInfo": {
            "host": "dummy",
            "response": {
                "headers": '{"apigw-requestid": "LY_66j0dPHcESCg=", "x-amzn-requestid": "x-amzn-requestid_LY_66j0dPHcESCg="}',
                "body": "",
                "statusCode": 200,
            },
        },
    }


@pytest.mark.parametrize(
    "uri, resource_name",
    [
        (
            "lambda.us-west-2.amazonaws.com/2015-03-31/functions/my-function/invocations?Qualifier=1",
            "my-function",
        ),
        (
            "lambda.eu-central-1.amazonaws.com/2015-03-31/functions/arn%3Aaws%3Alambda%3Aeu-central-1%3A123847209798%3Afunction%3Aservice-prod-accessRedis/invocations",
            "service-prod-accessRedis",
        ),
    ],
)
def test_lambda_parser_resource_name(uri, resource_name):
    parser = LambdaParser()
    params = HttpRequest(
        host="", method="POST", uri=uri, headers={}, body=json.dumps({"hello": "world"})
    )
    response = parser.parse_request(params)
    assert response["info"]["resourceName"] == resource_name


@pytest.mark.parametrize(
    "body",
    [
        b'<?xml version="1.0"?><SendMessageBatchResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/"><SendMessageBatchResult><SendMessageBatchResultEntry><Id>123</Id><MessageId>85dc3997-b060-47bc-9d89-c754d7260dbd</MessageId><MD5OfMessageBody>485b9ada0d1f06d60d71145304704c27</MD5OfMessageBody></SendMessageBatchResultEntry></SendMessageBatchResult><ResponseMetadata><RequestId>41295a06-b432-55b5-a8aa-00e764c8b9cf</RequestId></ResponseMetadata></SendMessageBatchResponse>',
        b'<?xml version="1.0"?><SendMessageResponse xmlns="http://queue.amazonAwsParser.com/doc/2012-11-05/"><SendMessageResult><MessageId>85dc3997-b060-47bc-9d89-c754d7260dbd</MessageId><MD5OfMessageBody>c5cb6abef11b88049177473a73ed662f</MD5OfMessageBody></SendMessageResult><ResponseMetadata><RequestId>b6b5a045-23c6-5e3a-a54f-f7dd99f7b379</RequestId></ResponseMetadata></SendMessageResponse>',
    ],
)
def test_sqs_parser_message_id(body):
    response = SqsParser().parse_response("dummy", 200, {}, body=body)
    assert response["info"]["messageId"] == "85dc3997-b060-47bc-9d89-c754d7260dbd"


@pytest.mark.parametrize(
    "method, body, message_id",
    [
        ("GetItem", {"TableName": "resourceName"}, None),
        (
            "PutItem",
            {"TableName": "resourceName", "Item": {"key": {"S": "value"}}},
            "1ad3dccc8064a706957c2c06ce3796bb",
        ),
        (
            "DeleteItem",
            {"TableName": "resourceName", "Key": {"key": {"S": "value"}}},
            "1ad3dccc8064a706957c2c06ce3796bb",
        ),
        (
            "UpdateItem",
            {"TableName": "resourceName", "Key": {"key": {"S": "value"}}},
            "1ad3dccc8064a706957c2c06ce3796bb",
        ),
        (
            "BatchWriteItem",
            {"RequestItems": {"resourceName": [{"PutRequest": {"Item": {"key": {"S": "value"}}}}]}},
            "1ad3dccc8064a706957c2c06ce3796bb",
        ),
        (
            "BatchWriteItem",
            {
                "RequestItems": {
                    "resourceName": [{"DeleteRequest": {"Key": {"key": {"S": "value"}}}}]
                }
            },
            "1ad3dccc8064a706957c2c06ce3796bb",
        ),
    ],
)
def test_dynamodb_parser_happy_flow(method, body, message_id):
    parser = DynamoParser()
    params = HttpRequest(
        host="",
        method="POST",
        uri="",
        headers={"x-amz-target": f"DynamoDB_20120810.{method}"},
        body=json.dumps(body),
    )
    response = parser.parse_request(params)
    assert response["info"]["resourceName"] == "resourceName"
    assert response["info"]["dynamodbMethod"] == method
    assert response["info"]["messageId"] == message_id


def test_dynamodb_parse_no_scrubbing():
    body = {"TableName": "component-test", "Key": {"field0": {"S": "1"}}}
    parser = DynamoParser()
    params = HttpRequest(
        host="",
        method="POST",
        uri="",
        headers={"x-amz-target": "DynamoDB_20120810.GetItem"},
        body=json.dumps(body),
    )
    response = parser.parse_request(params)
    assert json.loads(response["info"]["httpInfo"]["request"]["body"]) == body


def test_dynamodb_parser_sad_flow():
    parser = DynamoParser()
    params = HttpRequest(
        host="",
        method="POST",
        uri="",
        headers={"x-amz-target": "DynamoDB_20120810.GetItem"},
        body="not a json",
    )
    response = parser.parse_request(params)
    assert response["info"]["resourceName"] is None
    assert response["info"]["dynamodbMethod"] == "GetItem"
    assert response["info"]["messageId"] is None


def test_dynamodb_parser_sad_flow_unsupported_query():
    parser = DynamoParser()
    params = HttpRequest(
        host="",
        method="POST",
        uri="",
        headers={"x-amz-target": "DynamoDB_20120810.BatchWriteItem"},
        body='{"RequestItems": {}}',
    )
    with pytest.raises(Exception):
        parser.parse_request(params)


def test_double_response_size_limit_on_error_status_code():
    d = {"a": "v" * int(Configuration.get_max_entry_size() * 1.5)}
    info_no_error = Parser().parse_response("www.google.com", 200, d, json.dumps(d))
    response_no_error = info_no_error["info"]["httpInfo"]["response"]
    info_with_error = Parser().parse_response("www.google.com", 500, d, json.dumps(d))
    response_with_error = info_with_error["info"]["httpInfo"]["response"]

    assert len(response_with_error["headers"]) > len(response_no_error["headers"])
    assert response_with_error["headers"] == json.dumps(d)
    assert len(response_with_error["body"]) > len(response_no_error["body"])
    assert response_with_error["body"] == json.dumps(d)


def test_event_bridge_parser_request_happy_flow():
    parser = EventBridgeParser()
    params = HttpRequest(
        host="",
        method="POST",
        uri="",
        headers={},
        body=json.dumps(
            {
                "Entries": [
                    {
                        "Source": "source_lambda",
                        "Resources": [],
                        "DetailType": "string",
                        "Detail": '{"a": 1}',
                        "EventBusName": "name1",
                    },
                    {
                        "Source": "source_lambda",
                        "Resources": [],
                        "DetailType": "string",
                        "Detail": '{"a": 2}',
                        "EventBusName": "name1",
                    },
                    {
                        "Source": "source_lambda",
                        "Resources": [],
                        "DetailType": "string",
                        "Detail": '{"a": 3}',
                        "EventBusName": "name2",
                    },
                ]
            }
        ),
    )
    response = parser.parse_request(params)
    assert set(response["info"]["resourceNames"]) == {"name2", "name1"}


def test_event_bridge_parser_request_sad_flow():
    parser = EventBridgeParser()
    params = HttpRequest(host="", method="POST", uri="", headers={}, body="not a json")
    response = parser.parse_request(params)
    assert response["info"]["resourceNames"] is None


@pytest.mark.parametrize(
    "uri, resource_name, host",
    [
        (
            "s3.eu-west-1.amazonaws.com/my.s3-bucket1.com/documents/2021/3/31/file.pdf",
            "my.s3-bucket1.com",
            "s3.eu-west-1.amazonaws.com",
        ),
        (
            "my-s3-bucket.s3.us-west-2.amazonaws.com/documents/2021/3/31/file.pdf",
            "my-s3-bucket",
            "my-s3-bucket.s3.us-west-2.amazonaws.com",
        ),
    ],
)
def test_s3_parser_resource_name(uri, resource_name, host):
    parser = S3Parser()
    params = HttpRequest(
        host=host,
        method="PUT",
        uri=uri,
        headers={},
        body="",
    )
    response = parser.parse_request(params)
    assert response["info"]["resourceName"] == resource_name


def test_event_bridge_parser_response_happy_flow():
    parser = EventBridgeParser()
    response = parser.parse_response(
        "",
        200,
        {},
        body=json.dumps(
            {"Entries": [{"EventId": "1-2-3-4"}, {"EventId": "6-7-8-9"}], "FailedEntryCount": 0}
        ).encode(),
    )
    assert response["info"]["messageIds"] == ["1-2-3-4", "6-7-8-9"]


def test_event_bridge_parser_response_sad_flow():
    parser = EventBridgeParser()
    response = parser.parse_response("", 200, {}, body=b"not a json")
    assert not response["info"]["messageIds"]


def test_sns_parser_resource_name_topic_arn():
    parser = SnsParser()
    params = HttpRequest(
        host="host",
        method="PUT",
        uri="uri",
        headers={},
        body=b"TopicArn=arn:aws:sns:us-west-2:123456:sns-name",
    )
    response = parser.parse_request(params)
    assert response["info"]["resourceName"] == "arn:aws:sns:us-west-2:123456:sns-name"


def test_sns_parser_resource_name_target_arn():
    parser = SnsParser()
    params = HttpRequest(
        host="host",
        method="PUT",
        uri="uri",
        headers={},
        body=b"TargetArn=arn:aws:sns:us-west-2:123456:sns-name",
    )
    response = parser.parse_request(params)
    assert response["info"]["resourceName"] == "arn:aws:sns:us-west-2:123456:sns-name"


def test_base_parser_with_w3c():
    parser = Parser()
    params = HttpRequest(
        host="host",
        method="PUT",
        uri="uri",
        headers={
            TRACEPARENT_HEADER_NAME: "00-11111111111111111111111100000000-aaaaaaaaaaaaaaaa-01"
        },
        body=b"TargetArn=arn:aws:sns:us-west-2:123456:sns-name",
    )
    response = parser.parse_request(params)
    assert response["info"]["messageId"] == "aaaaaaaaaaaaaaaa"


def test_parser_w3c_weaker_then_other_message_id():
    """
    We want to make sure that if we have a collision - both a W3C messageId and a messageId from other parser,
     then we should use the other parser's MessageId.
    """
    parser = DynamoParser()
    params = HttpRequest(
        host="",
        method="POST",
        uri="",
        headers={
            "x-amz-target": "DynamoDB_20120810.PutItem",
            TRACEPARENT_HEADER_NAME: "00-11111111111111111111111100000000-aaaaaaaaaaaaaaaa-01",
        },
        body=json.dumps({"TableName": "resourceName", "Item": {"key": {"S": "value"}}}),
    )
    response = parser.parse_request(params)
    assert response["info"]["resourceName"] == "resourceName"
    assert response["info"]["dynamodbMethod"] == "PutItem"
    assert response["info"]["messageId"] == "1ad3dccc8064a706957c2c06ce3796bb"
