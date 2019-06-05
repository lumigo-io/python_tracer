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
)
from lumigo_tracer.utils import is_verbose
from .http_data_classes import HttpRequest

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
        if is_verbose():
            additional_info = {
                "headers": prepare_large_data(
                    dict(parse_params.headers.items() if parse_params.headers else {})
                ),
                "body": prepare_large_data(parse_params.body),
                "method": parse_params.method,
                "uri": parse_params.uri,
            }
        else:
            additional_info = {"method": parse_params.method}

        return {
            "id": str(uuid.uuid1()),
            "type": HTTP_TYPE,
            "info": {"httpInfo": {"host": parse_params.host, "request": additional_info}},
            "started": int(time.time() * 1000),
        }

    def parse_response(
        self, url: str, status_code: int, headers: Optional[http.client.HTTPMessage], body: bytes
    ) -> dict:
        if is_verbose():
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


class SqsParser(ServerlessAWSParser):
    def parse_request(self, parse_params: HttpRequest) -> dict:
        return recursive_json_join(
            super().parse_request(parse_params),
            {"info": {"resourceName": safe_key_from_query(parse_params.body, "QueueUrl")}},
        )


class S3Parser(ServerlessAWSParser):
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
