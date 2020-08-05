import json
import uuid
from typing import Type, Optional
import time

from lumigo_tracer.parsers.utils import (
    safe_split_get,
    safe_key_from_json,
    safe_key_from_xml,
    safe_key_from_query,
    recursive_json_join,
    safe_get,
    should_scrub_domain,
)
from lumigo_tracer.utils import Configuration, lumigo_dumps, md5hash, get_logger
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
                "headers": lumigo_dumps(parse_params.headers),
                "body": lumigo_dumps(parse_params.body) if parse_params.body else "",
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

    def parse_response(self, url: str, status_code: int, headers: dict, body: bytes) -> dict:
        if Configuration.verbose and not should_scrub_domain(url):
            additional_info = {
                "headers": lumigo_dumps(headers),
                "body": lumigo_dumps(body) if body else "",
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
        message_id = headers.get("x-amzn-requestid")
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

    @staticmethod
    def _extract_message_id(body: dict, method: str) -> Optional[str]:
        if method == "PutItem" and body.get("Item"):
            return md5hash(body["Item"])
        elif method in ("UpdateItem", "DeleteItem") and body.get("Key"):
            return md5hash(body["Key"])
        elif method == "BatchWriteItem" and body.get("RequestItems"):
            first_item = next(iter(body["RequestItems"].values()))
            if first_item:
                if first_item[0].get("PutRequest"):
                    return md5hash(first_item[0]["PutRequest"]["Item"])
                else:
                    return md5hash(first_item[0]["DeleteRequest"]["Key"])
        return None

    @staticmethod
    def _extract_table_name(body: dict, method: str) -> Optional[str]:
        name = body.get("TableName")
        if not name and method == "BatchWriteItem" and isinstance(body.get("RequestItems"), dict):
            return next(iter(body["RequestItems"]))
        return name

    def parse_request(self, parse_params: HttpRequest) -> dict:
        target: str = parse_params.headers.get("x-amz-target", "")
        method = safe_split_get(target, ".", 1)
        try:
            parsed_body = json.loads(parse_params.body)
        except json.JSONDecodeError as e:
            get_logger().debug("Error while trying to parse ddb request body", exc_info=e)
            parsed_body = {}

        return recursive_json_join(
            {
                "info": {
                    "resourceName": self._extract_table_name(parsed_body, method),
                    "dynamodbMethod": method,
                    "messageId": self._extract_message_id(parsed_body, method),
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
                "name": safe_split_get(str(parse_params.headers.get("path", "")), "/", 3),
                "invocationType": parse_params.headers.get("x-amz-invocation-type"),
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
        aws_request_id = headers.get("x-amzn-requestid")
        apigw_request_id = headers.get("apigw-requestid")
        message_id = aws_request_id or apigw_request_id
        return recursive_json_join(
            {"info": {"messageId": message_id}},
            super().parse_response(url, status_code, headers, body),
        )


def get_parser(url: str, headers: Optional[dict] = None) -> Type[Parser]:
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
    elif url.endswith("amazonaws.com") or (headers and headers.get("x-amzn-requestid")):
        return ServerlessAWSParser
    return Parser
