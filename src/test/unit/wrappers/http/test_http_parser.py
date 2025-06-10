import json
import re

import pytest
from lumigo_core.configuration import MASK_ALL_REGEX, CoreConfiguration
from lumigo_core.scrubbing import MASKED_SECRET

from lumigo_tracer.w3c_context import TRACEPARENT_HEADER_NAME
from lumigo_tracer.wrappers.http.http_data_classes import HttpRequest
from lumigo_tracer.wrappers.http.http_parser import (
    ApiGatewayV2Parser,
    DynamoParser,
    EventBridgeParser,
    KinesisParser,
    LambdaParser,
    Parser,
    S3Parser,
    ServerlessAWSParser,
    SnsParser,
    SqsJsonParser,
    SqsXmlParser,
    get_parser,
)


def test_serverless_aws_parser_fallback_doesnt_change():
    url = "https://kvpuorrsqb.execute-api.us-west-2.amazonaws.com"
    headers = {"nothing": "relevant"}
    serverless_parser = ServerlessAWSParser().parse_response(url, 200, headers=headers, body=b"")
    root_parser = Parser().parse_response(url, 200, headers=headers, body=b"")
    serverless_parser.pop("ended")
    root_parser.pop("ended")
    assert serverless_parser == root_parser


@pytest.mark.parametrize(
    "url, headers, expected_parser",
    [
        ("ne3kjv28fh.execute-api.us-west-2.amazonaws.com", {}, ApiGatewayV2Parser),
        ("s3.eu-west-1.amazonaws.com", {"key": "value"}, S3Parser),
        (
            "sqs.us-west-2.amazonaws.com",
            {"content-type": "application/x-amz-json-1.0"},
            SqsJsonParser,
        ),
        (
            "sqs.us-west-2.amazonaws.com",
            # This is a made up future version of the json protocol, to make sure that the SqsJsonParser
            # is still selected
            {"content-type": "application/x-amz-json-2.3.4"},
            SqsJsonParser,
        ),
        ("sqs.us-west-2.amazonaws.com", {}, SqsXmlParser),
        ("lambda.us-west-2.amazonaws.com", {}, LambdaParser),
        ("kinesis.us-west-2.amazonaws.com", {}, KinesisParser),
        ("events.us-west-2.amazonaws.com", {}, EventBridgeParser),
        ("sns.us-west-2.amazonaws.com", {}, SnsParser),
        # Non AWS Service
        ("events.other.service", {}, Parser),
        # If this header exists it should be detected as a ServerlessAWSParser
        ("api.dev.com", {"x-amzn-requestid": "1234"}, ServerlessAWSParser),
    ],
)
def test_get_parser(url, headers, expected_parser):
    assert get_parser(url, headers) == expected_parser


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
    "request_body",
    [
        # Send single message to SQS request
        b"QueueUrl=https%3A%2F%2Fsqs.us-west-2.amazonaws.com%2F449953265267%2Fsagivdeleteme-node-sqs"
        b"&MessageBody=%7B%0A%20%20%20%20%22body%22%3A%20%22test1%22%0A%7D"
        b"&MessageAttribute.1.Name=AttributeName"
        b"&MessageAttribute.1.Value.StringValue=Attribute%20Value"
        b"&MessageAttribute.1.Value.DataType=String"
        b"&Action=SendMessage"
        b"&Version=2012-11-05",
        # Send batch message request to SQS (on record in the batch)
        b"QueueUrl=https%3A%2F%2Fsqs.us-west-2.amazonaws.com%2F449953265267%2Fsagivdeleteme-node-sqs"
        b"&SendMessageBatchRequestEntry.1.Id=1"
        b"&SendMessageBatchRequestEntry.1.MessageBody=Message%201"
        b"&SendMessageBatchRequestEntry.2.Id=2"
        b"&SendMessageBatchRequestEntry.2.MessageBody=Message%202"
        b"&Action=SendMessageBatch"
        b"&Version=2012-11-05",
    ],
)
def test_sqs_xml_parse_resource_name(request_body):
    queue_url = "https://sqs.us-west-2.amazonaws.com/449953265267/sagivdeleteme-node-sqs"
    http_request = HttpRequest(host="dummy", method="POST", uri="", headers={}, body=request_body)
    parsed_request = SqsXmlParser().parse_request(http_request)
    assert parsed_request["info"]["resourceName"] == queue_url


@pytest.mark.parametrize(
    "body",
    [
        b'<?xml version="1.0"?><SendMessageBatchResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/"><SendMessageBatchResult><SendMessageBatchResultEntry><Id>123</Id><MessageId>85dc3997-b060-47bc-9d89-c754d7260dbd</MessageId><MD5OfMessageBody>485b9ada0d1f06d60d71145304704c27</MD5OfMessageBody></SendMessageBatchResultEntry></SendMessageBatchResult><ResponseMetadata><RequestId>41295a06-b432-55b5-a8aa-00e764c8b9cf</RequestId></ResponseMetadata></SendMessageBatchResponse>',
        b'<?xml version="1.0"?><SendMessageResponse xmlns="http://queue.amazonAwsParser.com/doc/2012-11-05/"><SendMessageResult><MessageId>85dc3997-b060-47bc-9d89-c754d7260dbd</MessageId><MD5OfMessageBody>c5cb6abef11b88049177473a73ed662f</MD5OfMessageBody></SendMessageResult><ResponseMetadata><RequestId>b6b5a045-23c6-5e3a-a54f-f7dd99f7b379</RequestId></ResponseMetadata></SendMessageResponse>',
    ],
)
def test_sqs_xml_parser_message_id(body):
    response = SqsXmlParser().parse_response("dummy", 200, {}, body=body)
    assert response["info"]["messageId"] == "85dc3997-b060-47bc-9d89-c754d7260dbd"


@pytest.mark.parametrize(
    "response_body, message_id",
    [
        (
            # Send single message to SQS response
            {
                "MD5OfMessageAttributes": "11111111111111111111111111111111",
                "MD5OfMessageBody": "11111111111111111111111111111111",
                "MessageId": "11111111-1111-1111-1111-111111111111",
            },
            "11111111-1111-1111-1111-111111111111",
        ),
        (
            # Send batch message request to SQS (one record in the batch), successful message
            {
                "Failed": [],
                "Successful": [
                    {
                        "Id": "1",
                        "MD5OfMessageBody": "11111111111111111111111111111111",
                        "MessageId": "11111111-1111-1111-1111-111111111111",
                    }
                ],
            },
            "11111111-1111-1111-1111-111111111111",
        ),
        (
            # Send batch message request to SQS (multiple record in the batch), successful message
            # Note: Currently we only send the first message id of a batch, but if there would be a need we will
            #       change this to send multiple message ids
            {
                "Failed": [],
                "Successful": [
                    {
                        "Id": "1",
                        "MD5OfMessageBody": "11111111111111111111111111111111",
                        "MessageId": "11111111-1111-1111-1111-111111111111",
                    },
                    {
                        "Id": "2",
                        "MD5OfMessageBody": "22222222222222222222222222222222",
                        "MessageId": "22222222-2222-2222-2222-222222222222",
                    },
                ],
            },
            "11111111-1111-1111-1111-111111111111",
        ),
        (
            # Send batch message request to SQS (one record in the batch), failed message
            # Note: Currently we only send the first message id of a batch, but if there would be a need we will
            #       change this to send multiple message ids
            {
                "Successful": [],
                "Failed": [
                    {
                        "Id": "1",
                        "MD5OfMessageBody": "11111111111111111111111111111111",
                        "MessageId": "11111111-1111-1111-1111-111111111111",
                    },
                    {
                        "Id": "2",
                        "MD5OfMessageBody": "22222222222222222222222222222222",
                        "MessageId": "22222222-2222-2222-2222-222222222222",
                    },
                ],
            },
            None,
        ),
        (
            # Send batch message request to SQS (many successful & many failed messages)
            # Note: Currently we only send the first successful message id of a batch, but if there would be a need
            #       we will change this to send multiple message ids
            {
                "Successful": [
                    {
                        "Id": "1",
                        "MD5OfMessageBody": "11111111111111111111111111111111",
                        "MessageId": "11111111-1111-1111-1111-111111111111",
                    },
                    {
                        "Id": "2",
                        "MD5OfMessageBody": "22222222222222222222222222222222",
                        "MessageId": "22222222-2222-2222-2222-222222222222",
                    },
                ],
                "Failed": [
                    {
                        "Id": "3",
                        "MD5OfMessageBody": "33333333333333333333333333333333",
                        "MessageId": "33333333-3333-3333-3333-333333333333",
                    },
                    {
                        "Id": "4",
                        "MD5OfMessageBody": "44444444444444444444444444444444",
                        "MessageId": "44444444-4444-4444-4444-444444444444",
                    },
                ],
            },
            "11111111-1111-1111-1111-111111111111",
        ),
        ({}, None),
    ],
)
def test_sqs_json_parse_message_id(response_body: dict, message_id):
    response_body_bytes = bytes(json.dumps(response_body), "utf-8")
    parsed_response = SqsJsonParser().parse_response(
        url="", status_code=200, headers={}, body=response_body_bytes
    )
    assert parsed_response["info"]["messageId"] == message_id


def test_sqs_json_parse_message_id_body_not_a_json():
    parsed_response = SqsJsonParser().parse_response(
        url="", status_code=200, headers={}, body=b"no a json"
    )
    assert parsed_response["info"]["messageId"] is None


@pytest.mark.parametrize(
    "request_body, queue_url",
    [
        (
            # Send single message to SQS request
            {
                "QueueUrl": "https://sqs.us-west-2.amazonaws.com/33/random-queue-test",
                "MessageBody": "This is a test message",
            },
            "https://sqs.us-west-2.amazonaws.com/33/random-queue-test",
        ),
        (
            # Send batch message request to SQS (on record in the batch)
            {
                "QueueUrl": "https://sqs.us-west-2.amazonaws.com/33/random-queue-test",
                "Entries": [{"Id": 1, "Message": "Message number 1"}],
            },
            "https://sqs.us-west-2.amazonaws.com/33/random-queue-test",
        ),
        (
            # Empty json response body
            {},
            None,
        ),
    ],
)
def test_sqs_json_parse_resource_name(request_body: dict, queue_url: str):
    request_body_bytes = bytes(json.dumps(request_body), "utf-8")
    http_request = HttpRequest(
        host="dummy", method="POST", uri="", headers={}, body=request_body_bytes
    )
    parsed_request = SqsJsonParser().parse_request(http_request)
    assert parsed_request["info"]["resourceName"] == queue_url


def test_sqs_json_parse_resource_name_body_not_a_json():
    http_request = HttpRequest(host="dummy", method="POST", uri="", headers={}, body=b"Not a json")
    parsed_request = SqsJsonParser().parse_request(http_request)
    assert parsed_request["info"]["resourceName"] is None


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
        (
            "BatchGetItem",
            {
                "RequestItems": {
                    "resourceName": [
                        {"Keys": {"Key": {"key": {"S": "value"}}}, "ConsistentRead": False}
                    ]
                }
            },
            None,
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
    d = {"a": "v" * int(CoreConfiguration.get_max_entry_size() * 1.5)}
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
    params = HttpRequest(host=host, method="PUT", uri=uri, headers={}, body="",)
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


@pytest.mark.parametrize(
    "input_uri, configs, expected_uri",
    [
        ("http://google.com", {}, "http://google.com"),
        ("http://google.com?query=param", {}, "http://google.com?query=param"),
        ("http://google.com?pass=1234&a=b", {}, "http://google.com?pass=----&a=b"),
        (
            "http://google.com?pass=1234&a=b",
            {"secret_masking_regex_http_query_params": re.compile("a")},
            "http://google.com?pass=1234&a=----",
        ),
        (
            "http://google.com?pass=1234&a=b",
            {"secret_masking_regex_http_query_params": MASK_ALL_REGEX},
            "http://google.com?pass=----&a=----",
        ),
    ],
)
def test_scrub_query_params(monkeypatch, input_uri, configs, expected_uri):
    for attr, value in configs.items():
        monkeypatch.setattr(CoreConfiguration, attr, value)

    response = Parser().parse_request(
        HttpRequest(host="host", method="PUT", uri=input_uri, headers={}, body=b"body",)
    )
    assert response["info"]["httpInfo"]["request"]["uri"] == expected_uri


def test_scrub_request(monkeypatch):
    monkeypatch.setattr(
        CoreConfiguration, "secret_masking_regex_http_request_bodies", re.compile("other")
    )
    monkeypatch.setattr(
        CoreConfiguration, "secret_masking_regex_http_request_headers", re.compile("bla")
    )

    response = Parser().parse_request(
        HttpRequest(
            host="host",
            method="PUT",
            uri="uri",
            headers={"bla": "1234", "other": "5678"},
            body=b'{"bla": "1234", "other": "5678"}',
        )
    )
    assert (
        response["info"]["httpInfo"]["request"]["body"]
        == f'{{"bla": "1234", "other": "{MASKED_SECRET}"}}'
    )
    assert (
        response["info"]["httpInfo"]["request"]["headers"]
        == f'{{"bla": "{MASKED_SECRET}", "other": "5678"}}'
    )


def test_scrub_response(monkeypatch):
    monkeypatch.setattr(
        CoreConfiguration, "secret_masking_regex_http_response_bodies", MASK_ALL_REGEX
    )
    monkeypatch.setattr(
        CoreConfiguration, "secret_masking_regex_http_response_headers", re.compile("bla")
    )

    response = Parser().parse_response(
        url="uri",
        status_code=200,
        headers={"bla": "1234", "other": "5678"},
        body=b'{"bla": "1234", "other": "5678"}',
    )
    assert response["info"]["httpInfo"]["response"]["body"] == MASKED_SECRET
    assert (
        response["info"]["httpInfo"]["response"]["headers"]
        == f'{{"bla": "{MASKED_SECRET}", "other": "5678"}}'
    )
