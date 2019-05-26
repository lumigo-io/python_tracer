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

    def parse_request(
        self, url: str, headers: Optional[http.client.HTTPMessage], body: bytes
    ) -> dict:
        if is_verbose():
            additional_info = {
                "headers": prepare_large_data(dict(headers.items() if headers else {})),
                "body": prepare_large_data(body),
            }
        else:
            additional_info = {}

        return {
            "id": str(uuid.uuid1()),
            "type": HTTP_TYPE,
            "info": {"httpInfo": {"host": url, "request": additional_info}},
            "started": int(time.time() * 1000),
        }

    def parse_response(
        self, url: str, headers: Optional[http.client.HTTPMessage], body: bytes
    ) -> dict:
        if is_verbose():
            additional_info = {
                "headers": prepare_large_data(dict(headers.items() if headers else {})),
                "body": prepare_large_data(body),
            }
        else:
            additional_info = {}

        return {
            "type": HTTP_TYPE,
            "info": {"httpInfo": {"host": url, "response": additional_info}},
            "ended": int(time.time() * 1000),
        }


class ServerlessAWSParser(Parser):
    def parse_response(self, url: str, headers, body: bytes) -> dict:
        return recursive_json_join(
            super().parse_response(url, headers, body),
            {"id": headers.get("x-amzn-requestid") or headers.get("x-amz-requestid")},
        )


class DynamoParser(ServerlessAWSParser):
    def parse_request(self, url: str, headers, body: bytes) -> dict:
        return recursive_json_join(
            super().parse_request(url, headers, body),
            {
                "info": {
                    "resourceName": safe_key_from_json(body, "TableName"),
                    "dynamodbMethod": safe_split_get(headers.get("x-amz-target", ""), ".", 1),
                }
            },
        )


class SnsParser(ServerlessAWSParser):
    def parse_request(self, url: str, headers, body: bytes) -> dict:
        return recursive_json_join(
            super().parse_request(url, headers, body),
            {
                "info": {
                    "resourceName": safe_key_from_query(body, "TopicArn"),
                    "targetArn": safe_key_from_query(body, "TopicArn"),
                }
            },
        )

    def parse_response(self, url: str, headers, body: bytes) -> dict:
        return recursive_json_join(
            super().parse_response(url, headers, body),
            {"messageId": safe_key_from_xml(body, "PublishResponse/PublishResult/MessageId")},
        )


class LambdaParser(ServerlessAWSParser):
    def parse_request(self, url: str, headers, body: bytes) -> dict:
        return recursive_json_join(
            super().parse_request(url, headers, body),
            {
                "name": safe_split_get(headers.get("path", ""), "/", 3),
                "invocationType": headers.get("x-amz-invocation-type"),
            },
        )


class KinesisParser(ServerlessAWSParser):
    def parse_request(self, url: str, headers, body: bytes) -> dict:
        return recursive_json_join(
            super().parse_request(url, headers, body),
            {"info": {"resourceName": safe_key_from_json(body, "StreamName")}},
        )


class SqsParser(ServerlessAWSParser):
    def parse_request(self, url: str, headers, body: bytes) -> dict:
        return recursive_json_join(
            super().parse_request(url, headers, body),
            {"info": {"resourceName": safe_key_from_query(body, "QueueUrl")}},
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
    # SQS Legacy Endpoints: https://docs.aws.amazon.com/general/latest/gr/rande.html
    elif service in ("sqs", "sqs-fips") or url.endswith("queue.amazonaws.com"):
        return SqsParser
    return Parser
