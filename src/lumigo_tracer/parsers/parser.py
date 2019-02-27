import uuid
from typing import Type
import time

from lumigo_tracer.parsers.utils import (
    safe_split_get,
    safe_key_from_json,
    safe_key_from_xml,
    safe_key_from_query,
    recursive_json_join,
)


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

    def parse_request(self, url: str, headers, body: bytes) -> dict:
        return {
            "id": str(uuid.uuid1()),
            "type": "http",
            "info": {"httpInfo": {"host": url}},
            "timestamp": int(time.time() * 1000),
        }

    def parse_response(self, url: str, headers, body: bytes) -> dict:
        return {
            "type": "http",
            "info": {"httpInfo": {"host": url}},
            "timestamp": int(time.time() * 1000),
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
                "name": safe_key_from_json(body, "TableName"),
                "dynamodbMethod": safe_split_get(headers.get("x-amz-target", ""), ".", 1),
            },
        )


class SnsParser(ServerlessAWSParser):
    def parse_request(self, url: str, headers, body: bytes) -> dict:
        return recursive_json_join(
            super().parse_request(url, headers, body),
            {
                "name": safe_key_from_query(body, "TargetArn"),
                "targetArn": safe_key_from_query(body, "TargetArn"),
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


def get_parser(url: str) -> Type[Parser]:
    service = safe_split_get(url, ".", 0)
    if service == "dynamodb":
        return DynamoParser
    elif service == "sns":
        return SnsParser
    elif service == "lambda":
        return LambdaParser
    return Parser
