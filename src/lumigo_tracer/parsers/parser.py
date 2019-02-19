from typing import Type
import time

from lumigo_tracer.parsers.utils import safe_split_get, key_from_json, key_from_xml, key_from_query


class Parser:
    """
    This parser class in the root parser of all the specific parser.
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
        return {"type": "http", "url": url, "timestamp": int(time.time() * 1000)}

    def parse_response(self, url: str, headers, body: bytes) -> dict:
        return {"type": "http", "url": url, "timestamp": int(time.time() * 1000)}


class ServerlessAWSParser(Parser):
    def parse_request(self, url: str, headers, body: bytes) -> dict:
        return {
            **super().parse_request(url, headers, body),
            **{"service": safe_split_get(url, ".", 0), "region": safe_split_get(url, ".", 1)},
        }

    def parse_response(self, url: str, headers, body: bytes) -> dict:
        return {
            **super().parse_response(url, headers, body),
            **{"id": headers.get("x-amzn-requestid") or headers.get("x-amz-requestid")},
        }


class DynamoParser(ServerlessAWSParser):
    def parse_request(self, url: str, headers, body: bytes) -> dict:
        return {
            **super().parse_request(url, headers, body),
            **{
                "name": key_from_json(body, "TableName"),
                "dynamodbMethod": safe_split_get(headers.get("x-amz-target", ""), ".", 1),
            },
        }


class SnsParser(ServerlessAWSParser):
    def parse_request(self, url: str, headers, body: bytes) -> dict:
        return {
            **super().parse_request(url, headers, body),
            **{
                "name": key_from_query(body, "TargetArn"),
                "targetArn": key_from_query(body, "TargetArn"),
            },
        }

    def parse_response(self, url: str, headers, body: bytes) -> dict:
        return {
            **super().parse_response(url, headers, body),
            **{"messageId": key_from_xml(body, "PublishResponse/PublishResult/MessageId")},
        }


class LambdaParser(ServerlessAWSParser):
    def parse_request(self, url: str, headers, body: bytes) -> dict:
        return {
            **super().parse_request(url, headers, body),
            **{
                "name": safe_split_get(headers.get("path", ""), "/", 3),
                "invocationType": headers.get("x-amz-invocation-type"),
            },
        }


def get_parser(url: str) -> Type[Parser]:
    service = safe_split_get(url, ".", 0)
    if service == "dynamodb":
        return DynamoParser
    elif service == "sns":
        return SnsParser
    elif service == "lambda":
        return LambdaParser
    return Parser
