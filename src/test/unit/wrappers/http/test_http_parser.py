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
            {"Content-Type": "application/x-amz-json-1.0"},
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
            b"{"
            b'   "MD5OfMessageAttributes":"6e6aba56e93b3ddfdfe3fa28895feece",'  # pragma: allowlist secret
            b'   "MD5OfMessageBody":"0d40eb1479f7e61b1a1c7a425c3949e4",'  # pragma: allowlist secret
            b'   "MessageId":"c5aca29a-ff2f-4db5-94c3-90523d1ed4ca"'  # pragma: allowlist secret
            b"}",
            "c5aca29a-ff2f-4db5-94c3-90523d1ed4ca",
        ),
        (
            # Send batch message request to SQS (one record in the batch), successful message
            b"{"
            b'   "Failed":[],'
            b'   "Successful":['
            b"     {"
            b'        "Id":"1",'
            b'        "MD5OfMessageBody":"68390233272823b7adf13a1db79b2cd7",'  # pragma: allowlist secret
            b'        "MessageId":"c5aca29a-ff2f-4db5-94c3-90523d1ed4ca"'
            b"     }"
            b"]}",
            "c5aca29a-ff2f-4db5-94c3-90523d1ed4ca",
        ),
        (
            # Send batch message request to SQS (multiple record in the batch), successful message
            # Note: Currently we only send the first message id of a batch, but if there would be a need we will change
            #       this to send multiple message ids
            b"{"
            b'   "Failed":[],'
            b'   "Successful":['
            b"     {"
            b'        "Id":"1",'
            b'        "MD5OfMessageBody":"68390233272823b7adf13a1db79b2cd7",'  # pragma: allowlist secret
            b'        "MessageId":"c5aca29a-ff2f-4db5-94c3-90523d1ed4ca"'
            b"     },"
            b"     {"
            b'        "Id":"2",'
            b'        "MD5OfMessageBody":"68390233272823b7adf13a1db7222222",'  # pragma: allowlist secret
            b'        "MessageId":"c5aca29a-ff2f-4db5-94c3-90523d222222"'
            b"     }"
            b"]}",
            "c5aca29a-ff2f-4db5-94c3-90523d1ed4ca",
        ),
        (
            # Send batch message request to SQS (one record in the batch), failed message
            # Note: Currently we only send the first message id of a batch, but if there would be a need we will change
            #       this to send multiple message ids
            b"{"
            b'   "Successful":[],'
            b'   "Failed":['
            b"     {"
            b'        "Id":"1",'
            b'        "MD5OfMessageBody":"68390233272823b7adf13a1db79b2cd7",'  # pragma: allowlist secret
            b'        "MessageId":"c5aca29a-ff2f-4db5-94c3-90523d1ed4ca"'
            b"     },"
            b"     {"
            b'        "Id":"2",'
            b'        "MD5OfMessageBody":"68390233272823b7adf13a1db7222222",'  # pragma: allowlist secret
            b'        "MessageId":"c5aca29a-ff2f-4db5-94c3-90523d222222"'
            b"     }"
            b"]}",
            None,
        ),
        (
            # Send batch message request to SQS (many successful & many failed messages)
            # Note: Currently we only send the first successful message id of a batch, but if there would be a need
            #       we will change this to send multiple message ids
            b"{"
            b'   "Successful":['
            b"     {"
            b'        "Id":"1",'
            b'        "MD5OfMessageBody":"68390233272823b7adf13a1db79b2cd7",'  # pragma: allowlist secret
            b'        "MessageId":"c5aca29a-ff2f-4db5-94c3-90523d1ed4ca"'
            b"     },"
            b"     {"
            b'        "Id":"2",'
            b'        "MD5OfMessageBody":"68390233272823b7adf13a1db7222222",'  # pragma: allowlist secret
            b'        "MessageId":"c5aca29a-ff2f-4db5-94c3-90523d222222"'
            b"     }],"
            b'   "Failed":['
            b"     {"
            b'        "Id":"3",'
            b'        "MD5OfMessageBody":"68390233272823b7adf13a1db7333333",'  # pragma: allowlist secret
            b'        "MessageId":"c5aca29a-ff2f-4db5-94c3-90523d333333"'
            b"     },"
            b"     {"
            b'        "Id":"4",'
            b'        "MD5OfMessageBody":"68390233272823b7adf13a1db7444444",'  # pragma: allowlist secret
            b'        "MessageId":"c5aca29a-ff2f-4db5-94c3-90523d444444"'
            b"     }"
            b"]}",
            "c5aca29a-ff2f-4db5-94c3-90523d1ed4ca",
        ),
        (b"{}", None),
        (b"Not a json", None),
    ],
)
def test_sqs_json_parse_message_id(response_body: bytes, message_id):
    parsed_response = SqsJsonParser().parse_response(
        url="", status_code=200, headers={}, body=response_body
    )
    assert parsed_response["info"]["messageId"] == message_id


@pytest.mark.parametrize(
    "request_body, queue_url",
    [
        (
            # Send single message to SQS request
            b"{"
            b'   "QueueUrl": "https://sqs.us-west-2.amazonaws.com/33/random-queue-test", '
            b'   "MessageBody": "This is a test message"'
            b"}",
            "https://sqs.us-west-2.amazonaws.com/33/random-queue-test",
        ),
        (
            # Send batch message request to SQS (on record in the batch)
            b"{"
            b'   "QueueUrl": "https://sqs.us-west-2.amazonaws.com/33/random-queue-test", '
            b'   "Entries": ['
            b'     {"Id": 1, "Message": "Message number 1"}'
            b"   ]"
            b"}",
            "https://sqs.us-west-2.amazonaws.com/33/random-queue-test",
        ),
        (
            # Empty json response body
            b"{}",
            None,
        ),
        (
            # Not a json body
            b"This is not a json body",
            None,
        ),
    ],
)
def test_sqs_json_parse_resource_name(request_body: bytes, queue_url: str):
    http_request = HttpRequest(host="dummy", method="POST", uri="", headers={}, body=request_body)
    parsed_request = SqsJsonParser().parse_request(http_request)
    assert parsed_request["info"]["resourceName"] == queue_url


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
        HttpRequest(
            host="host",
            method="PUT",
            uri=input_uri,
            headers={},
            body=b"body",
        )
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
