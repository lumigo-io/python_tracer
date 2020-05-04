import uuid
from typing import Type, Optional
import time
import http.client

from lumigo_tracer.parsers.utils import (
    safe_split_get,
    safe_key_from_json,
    safe_key_from_xml,
    safe_key_from_query,
    recursive_json_join,
    safe_get,
    should_scrub_domain,
)
from lumigo_tracer.utils import Configuration, prepare_large_data, omit_keys
from lumigo_tracer.parsers.http_data_classes import HttpRequest

HTTP_TYPE = "http"


class Parser:
    """
    This parser class is the root parser of all the specific parser.
    We parse our messages using the following hierarchical structure:

    --- Parser --\
                 |--- ServerlessAWSParser --\
                 |                          | ---DynamoParser
                 |                          | ---SnsParser
                 |                          | ---LambdaParser
                 |
                 |----- <FutureParser> ----\
    """

    def parse_request(self, parse_params: HttpRequest) -> dict:
        if Configuration.verbose and parse_params and not should_scrub_domain(parse_params.host):
            additional_info = {
                "headers": prepare_large_data(
                    omit_keys(dict(parse_params.headers.items() if parse_params.headers else {}))
                ),
                "body": prepare_large_data(omit_keys(parse_params.body)),
                "method": parse_params.method,
                "uri": parse_params.uri,
            }
        else:
            additional_info = {
                "method": parse_params.method if parse_params else "",
                "body": "The data is not available",
            }

        return {
            "id": str(uuid.uuid4()),
            "type": HTTP_TYPE,
            "info": {
                "httpInfo": {
                    "host": parse_params.host if parse_params else "",
                    "request": additional_info,
                }
            },
            "started": int(time.time() * 1000),
        }

    def parse_response(
        self, url: str, status_code: int, headers: Optional[http.client.HTTPMessage], body: bytes
    ) -> dict:
        if Configuration.verbose and not should_scrub_domain(url):
            additional_info = {
                "headers": prepare_large_data(omit_keys(dict(headers.items() if headers else {}))),
                "body": prepare_large_data(omit_keys(body)),
                "statusCode": status_code,
            }
        else:
            additional_info = {"statusCode": status_code, "body": "The data is not available"}

        return {
            "type": HTTP_TYPE,
            "info": {"httpInfo": {"host": url, "response": additional_info}},
            "ended": int(time.time() * 1000),
        }


class ServerlessAWSParser(Parser):
    # Override this field to add message id using the amz headers
    should_add_message_id = True

    def parse_response(self, url: str, status_code: int, headers, body: bytes) -> dict:
        additional_info = {}
        message_id = headers.get("x-amzn-RequestId")
        if message_id and self.should_add_message_id:
            additional_info["info"] = {"messageId": message_id}
        span_id = headers.get("x-amzn-requestid") or headers.get("x-amz-requestid")
        if span_id:
            additional_info["id"] = span_id
        return recursive_json_join(
            additional_info, super().parse_response(url, status_code, headers, body)
        )


class DynamoParser(ServerlessAWSParser):
    should_add_message_id = False

    def parse_request(self, parse_params: HttpRequest) -> dict:
        target: str = str(parse_params.headers.get("x-amz-target", ""))  # type: ignore
        return recursive_json_join(
            {
                "info": {
                    "resourceName": safe_key_from_json(parse_params.body, "TableName"),
                    "dynamodbMethod": safe_split_get(target, ".", 1),
                }
            },
            super().parse_request(parse_params),
        )


class SnsParser(ServerlessAWSParser):
    def parse_request(self, parse_params: HttpRequest) -> dict:
        return recursive_json_join(
            {
                "info": {
                    "resourceName": safe_key_from_query(parse_params.body, "TopicArn"),
                    "targetArn": safe_key_from_query(parse_params.body, "TopicArn"),
                }
            },
            super().parse_request(parse_params),
        )

    def parse_response(self, url: str, status_code: int, headers, body: bytes) -> dict:
        return recursive_json_join(
            {
                "info": {
                    "messageId": safe_key_from_xml(body, "PublishResponse/PublishResult/MessageId")
                }
            },
            super().parse_response(url, status_code, headers, body),
        )


class LambdaParser(ServerlessAWSParser):
    def parse_request(self, parse_params: HttpRequest) -> dict:
        return recursive_json_join(
            {
                "name": safe_split_get(
                    str(parse_params.headers.get("path", "")), "/", 3  # type: ignore
                ),
                "invocationType": parse_params.headers.get("x-amz-invocation-type"),  # type: ignore
            },
            super().parse_request(parse_params),
        )


class KinesisParser(ServerlessAWSParser):
    def parse_request(self, parse_params: HttpRequest) -> dict:
        return recursive_json_join(
            {"info": {"resourceName": safe_key_from_json(parse_params.body, "StreamName")}},
            super().parse_request(parse_params),
        )

    def parse_response(self, url: str, status_code: int, headers, body: bytes) -> dict:
        return recursive_json_join(
            {"info": {"messageId": KinesisParser._extract_message_id(body)}},
            super().parse_response(url, status_code, headers, body),
        )

    @staticmethod
    def _extract_message_id(response_body: bytes) -> Optional[str]:
        return safe_key_from_json(response_body, "SequenceNumber") or safe_get(  # type: ignore
            safe_key_from_json(response_body, "Records", []), [0, "SequenceNumber"]
        )


class SqsParser(ServerlessAWSParser):
    def parse_request(self, parse_params: HttpRequest) -> dict:
        return recursive_json_join(
            {"info": {"resourceName": safe_key_from_query(parse_params.body, "QueueUrl")}},
            super().parse_request(parse_params),
        )

    def parse_response(self, url: str, status_code: int, headers, body: bytes) -> dict:
        return recursive_json_join(
            {"info": {"messageId": SqsParser._extract_message_id(body)}},
            super().parse_response(url, status_code, headers, body),
        )

    @staticmethod
    def _extract_message_id(response_body: bytes) -> Optional[str]:
        return safe_key_from_xml(
            response_body, "SendMessageResponse/SendMessageResult/MessageId"  # Single.
        ) or safe_key_from_xml(
            response_body,
            "SendMessageBatchResponse/SendMessageBatchResult/SendMessageBatchResultEntry/0/MessageId",  # Batch.
        )


class S3Parser(Parser):
    def parse_request(self, parse_params: HttpRequest) -> dict:
        return recursive_json_join(
            {"info": {"resourceName": safe_split_get(parse_params.host, ".", 0)}},
            super().parse_request(parse_params),
        )

    def parse_response(self, url: str, status_code: int, headers, body: bytes) -> dict:
        return recursive_json_join(
            {"info": {"messageId": headers.get("x-amz-request-id")}},
            super().parse_response(url, status_code, headers, body),
        )


class StepFunctionParser(ServerlessAWSParser):
    def create_span(self, message_id: str) -> dict:
        return recursive_json_join(
            {
                "info": {
                    "resourceName": "StepFunction",
                    "httpInfo": {"host": "StepFunction"},
                    "messageId": message_id,
                }
            },
            super().parse_request(None),  # type: ignore
        )


class ApiGatewayV2Parser(ServerlessAWSParser):
    # API-GW V1 covered by ServerlessAWSParser

    def parse_response(self, url: str, status_code: int, headers, body: bytes) -> dict:
        aws_request_id = headers.get("x-amzn-RequestId")
        apigw_request_id = headers.get("Apigw-Requestid")
        message_id = aws_request_id or apigw_request_id
        return recursive_json_join(
            {"info": {"messageId": message_id}},
            super().parse_response(url, status_code, headers, body),
        )


def get_parser(url: str, headers: Optional[http.client.HTTPMessage] = None) -> Type[Parser]:
    service = safe_split_get(url, ".", 0)
    if service == "dynamodb":
        return DynamoParser
    elif service == "sns":
        return SnsParser
    elif service == "lambda":
        return LambdaParser
    elif service == "kinesis":
        return KinesisParser
    elif safe_split_get(url, ".", 1) == "s3":
        return S3Parser
    # SQS Legacy Endpoints: https://docs.aws.amazon.com/general/latest/gr/rande.html
    elif service in ("sqs", "sqs-fips") or "queue.amazonaws.com" in url:
        return SqsParser
    elif "execute-api" in url:
        return ApiGatewayV2Parser
    elif url.endswith("amazonaws.com") or (headers and headers.get("x-amzn-RequestId")):
        return ServerlessAWSParser
    return Parser
