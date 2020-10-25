import json

import pytest
from lumigo_tracer.lumigo_utils import Configuration

from lumigo_tracer.wrappers.http.http_data_classes import HttpRequest
from lumigo_tracer.wrappers.http.http_parser import (
    ServerlessAWSParser,
    Parser,
    get_parser,
    ApiGatewayV2Parser,
    DynamoParser,
    EventBridgeParser,
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
