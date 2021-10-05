import json
import uuid
from typing import Type, Optional, List
from urllib.parse import unquote

from lumigo_tracer.parsing_utils import (
    safe_split_get,
    safe_key_from_json,
    safe_key_from_xml,
    safe_key_from_query,
    recursive_json_join,
    safe_get,
    should_scrub_domain,
    extract_function_name_from_arn,
)
from lumigo_tracer.lumigo_utils import (
    Configuration,
    lumigo_dumps,
    md5hash,
    get_logger,
    get_current_ms_time,
    is_error_code,
    is_aws_arn,
)
from lumigo_tracer.wrappers.http.http_data_classes import HttpRequest, HttpState

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
            HttpState.omit_skip_path = self.get_omit_skip_path()
            additional_info = {
                "headers": lumigo_dumps(parse_params.headers),
                "body": lumigo_dumps(parse_params.body, omit_skip_path=HttpState.omit_skip_path)
                if parse_params.body
                else "",
                "method": parse_params.method,
                "uri": parse_params.uri,
                "instance_id": parse_params.instance_id,
            }
        else:
            additional_info = {
                "method": parse_params.method if parse_params else "",
                "body": "The data is not available",
                "instance_id": parse_params.instance_id,
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
            "started": get_current_ms_time(),
        }

    def parse_response(self, url: str, status_code: int, headers: dict, body: bytes) -> dict:
        max_size = Configuration.get_max_entry_size(has_error=is_error_code(status_code))
        if Configuration.verbose and not should_scrub_domain(url):
            additional_info = {
                "headers": lumigo_dumps(headers, max_size),
                "body": lumigo_dumps(body, max_size) if body else "",
                "statusCode": status_code,
            }
        else:
            additional_info = {"statusCode": status_code, "body": "The data is not available"}

        return {
            "type": HTTP_TYPE,
            "info": {"httpInfo": {"host": url, "response": additional_info}},
            "ended": get_current_ms_time(),
        }

    @staticmethod
    def get_omit_skip_path() -> Optional[List[str]]:
        return None


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

    @staticmethod
    def get_omit_skip_path() -> Optional[List[str]]:
        return ["Key"]


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
        decoded_uri = safe_split_get(unquote(parse_params.uri), "/", 3)
        return recursive_json_join(
            {
                "info": {
                    "resourceName": extract_function_name_from_arn(decoded_uri)
                    if is_aws_arn(decoded_uri)
                    else decoded_uri
                },
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
            safe_key_from_json(response_body, "Records", []), [0, "SequenceNumber"]  # type: ignore
        )

    @staticmethod
    def get_omit_skip_path() -> Optional[List[str]]:
        return ["PartitionKey"]


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
        resource_name = safe_split_get(parse_params.host, ".", 0)
        if resource_name == "s3":
            resource_name = safe_split_get(parse_params.uri, "/", 1)
        return recursive_json_join(
            {"info": {"resourceName": resource_name}},
            super().parse_request(parse_params),
        )

    def parse_response(self, url: str, status_code: int, headers, body: bytes) -> dict:
        return recursive_json_join(
            {"info": {"messageId": headers.get("x-amz-request-id")}},
            super().parse_response(url, status_code, headers, body),
        )


class EventBridgeParser(Parser):
    def parse_request(self, parse_params: HttpRequest) -> dict:
        try:
            parsed_body = json.loads(parse_params.body)
        except json.JSONDecodeError as e:
            get_logger().exception(
                "Error while trying to parse eventBridge request body", exc_info=e
            )
            parsed_body = {}
        resource_names = set()
        if isinstance(parsed_body.get("Entries"), list):
            resource_names = {
                e["EventBusName"] for e in parsed_body["Entries"] if e.get("EventBusName")
            }
        return recursive_json_join(
            {"info": {"resourceNames": list(resource_names) or None}},
            super().parse_request(parse_params),
        )

    def parse_response(self, url: str, status_code: int, headers, body: bytes) -> dict:
        try:
            parsed_body = json.loads(body)
        except json.JSONDecodeError as e:
            get_logger().debug("Error while trying to parse eventBridge request body", exc_info=e)
            parsed_body = {}
        message_ids = []
        if isinstance(parsed_body.get("Entries"), list):
            message_ids = [e["EventId"] for e in parsed_body["Entries"] if e.get("EventId")]
        return recursive_json_join(
            {"info": {"messageIds": message_ids}},
            super().parse_response(url, status_code, headers, body),
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
    elif service == "events":
        return EventBridgeParser
    elif safe_split_get(url, ".", 1) == "s3" or safe_split_get(url, ".", 0) == "s3":
        return S3Parser
    # SQS Legacy Endpoints: https://docs.aws.amazon.com/general/latest/gr/rande.html
    elif service in ("sqs", "sqs-fips") or "queue.amazonaws.com" in url:
        return SqsParser
    elif "execute-api" in url:
        return ApiGatewayV2Parser
    elif url.endswith("amazonaws.com") or (headers and headers.get("x-amzn-requestid")):
        return ServerlessAWSParser
    return Parser
