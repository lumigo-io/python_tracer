import json

import pytest

from lumigo_tracer.parsers.http_data_classes import HttpRequest
from lumigo_tracer.parsers.parser import (
    ServerlessAWSParser,
    Parser,
    get_parser,
    ApiGatewayV2Parser,
    DynamoParser,
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
    url = "api.rti.dev.toyota.com"
    headers = {"x-amzn-requestid": "1234"}
    assert get_parser(url, headers) == ServerlessAWSParser


def test_get_parser_apigw():
    url = "https://ne3kjv28fh.execute-api.us-west-2.amazonaws.com/doriaviram"
    assert get_parser(url, {}) == ApiGatewayV2Parser


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
                "body": "null",
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
                "body": "null",
                "statusCode": 200,
            },
        },
    }


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
