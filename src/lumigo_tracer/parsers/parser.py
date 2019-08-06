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
    prepare_large_data,
    safe_get,
)
from lumigo_tracer.utils import Configuration
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
        if Configuration.verbose and parse_params:
            additional_info = {
                "headers": prepare_large_data(
                    dict(parse_params.headers.items() if parse_params.headers else {})
                ),
                "body": prepare_large_data(parse_params.body),
                "method": parse_params.method,
                "uri": parse_params.uri,
            }
        else:
            additional_info = {"method": parse_params.method if parse_params else ""}

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
        if Configuration.verbose:
            additional_info = {
                "headers": prepare_large_data(dict(headers.items() if headers else {})),
                "body": prepare_large_data(body),
                "statusCode": status_code,
            }
        else:
            additional_info = {"statusCode": status_code}

        return {
            "type": HTTP_TYPE,
            "info": {"httpInfo": {"host": url, "response": additional_info}},
            "ended": int(time.time() * 1000),
        }


class ServerlessAWSParser(Parser):
    def parse_response(self, url: str, status_code: int, headers, body: bytes) -> dict:
        return recursive_json_join(
            super().parse_response(url, status_code, headers, body),
            {"id": headers.get("x-amzn-requestid") or headers.get("x-amz-requestid")},
        )


class DynamoParser(ServerlessAWSParser):
    def parse_request(self, parse_params: HttpRequest) -> dict:
        target: str = str(parse_params.headers.get("x-amz-target", ""))  # type: ignore
        return recursive_json_join(
            super().parse_request(parse_params),
            {
                "info": {
                    "resourceName": safe_key_from_json(parse_params.body, "TableName"),
                    "dynamodbMethod": safe_split_get(target, ".", 1),
                }
            },
        )


class SnsParser(ServerlessAWSParser):
    def parse_request(self, parse_params: HttpRequest) -> dict:
        return recursive_json_join(
            super().parse_request(parse_params),
            {
                "info": {
                    "resourceName": safe_key_from_query(parse_params.body, "TopicArn"),
                    "targetArn": safe_key_from_query(parse_params.body, "TopicArn"),
                }
            },
        )

    def parse_response(self, url: str, status_code: int, headers, body: bytes) -> dict:
        return recursive_json_join(
            super().parse_response(url, status_code, headers, body),
            {
                "info": {
                    "messageId": safe_key_from_xml(body, "PublishResponse/PublishResult/MessageId")
                }
            },
        )


class LambdaParser(ServerlessAWSParser):
    def parse_request(self, parse_params: HttpRequest) -> dict:
        return recursive_json_join(
            super().parse_request(parse_params),
            {
                "name": safe_split_get(
                    str(parse_params.headers.get("path", "")), "/", 3  # type: ignore
                ),
                "invocationType": parse_params.headers.get("x-amz-invocation-type"),  # type: ignore
            },
        )


class KinesisParser(ServerlessAWSParser):
    def parse_request(self, parse_params: HttpRequest) -> dict:
        return recursive_json_join(
            super().parse_request(parse_params),
            {"info": {"resourceName": safe_key_from_json(parse_params.body, "StreamName")}},
        )

    def parse_response(self, url: str, status_code: int, headers, body: bytes) -> dict:
        return recursive_json_join(
            super().parse_response(url, status_code, headers, body),
            {"info": {"messageId": KinesisParser._extract_message_id(body)}},
        )

    @staticmethod
    def _extract_message_id(response_body: bytes) -> Optional[str]:
        return safe_key_from_json(response_body, "SequenceNumber") or safe_get(  # type: ignore
            safe_key_from_json(response_body, "Records", []), [0, "SequenceNumber"]
        )


class SqsParser(ServerlessAWSParser):
    def parse_request(self, parse_params: HttpRequest) -> dict:
        return recursive_json_join(
            super().parse_request(parse_params),
            {"info": {"resourceName": safe_key_from_query(parse_params.body, "QueueUrl")}},
        )

    def parse_response(self, url: str, status_code: int, headers, body: bytes) -> dict:
        return recursive_json_join(
            super().parse_response(url, status_code, headers, body),
            {"info": {"messageId": SqsParser._extract_message_id(body)}},
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
            super().parse_request(parse_params),
            {"info": {"resourceName": safe_split_get(parse_params.host, ".", 0)}},
        )

    def parse_response(self, url: str, status_code: int, headers, body: bytes) -> dict:
        return recursive_json_join(
            super().parse_response(url, status_code, headers, body),
            {"info": {"messageId": headers.get("x-amz-request-id")}},
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


def get_parser(url: str) -> Type[Parser]:
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
    return Parser
