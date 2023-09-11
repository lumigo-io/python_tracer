import json
import uuid
from typing import Any, Dict, List, Optional, Type
from urllib.parse import parse_qsl, unquote, urlencode, urlparse, urlunparse

from lumigo_core.configuration import CoreConfiguration
from lumigo_core.lumigo_utils import md5hash
from lumigo_core.parsing_utils import (
    extract_function_name_from_arn,
    recursive_json_join,
    safe_get,
    safe_key_from_json,
    safe_key_from_query,
    safe_key_from_xml,
    safe_split_get,
)
from lumigo_core.scrubbing import get_omitting_regex

from lumigo_tracer.lumigo_utils import (
    Configuration,
    get_current_ms_time,
    get_logger,
    is_aws_arn,
    is_error_code,
    lumigo_dumps_with_context,
    lumigo_safe_execute,
    should_use_tracer_extension,
)
from lumigo_tracer.parsing_utils import should_scrub_domain
from lumigo_tracer.w3c_context import get_w3c_message_id, is_w3c_headers
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

    def parse_request(self, parse_params: HttpRequest) -> dict:  # type: ignore[type-arg]
        if Configuration.verbose and parse_params and not should_scrub_domain(parse_params.host):
            HttpState.omit_skip_path = self.get_omit_skip_path()
            additional_info = {
                "headers": lumigo_dumps_with_context("requestHeaders", parse_params.headers),
                "body": lumigo_dumps_with_context(
                    "requestBody", parse_params.body, omit_skip_path=HttpState.omit_skip_path
                )
                if parse_params.body and not Configuration.skip_collecting_http_body
                else "",
                "method": parse_params.method,
                "uri": self.scrub_query_params(parse_params.uri) or "",
                "instance_id": parse_params.instance_id,
            }
        else:
            additional_info = {
                "method": parse_params.method if parse_params else "",
                "body": "The data is not available",
                "instance_id": parse_params.instance_id,
            }

        message_id = None
        if parse_params.headers and is_w3c_headers(parse_params.headers):
            if not parse_params.host or not parse_params.host.startswith("lambda."):
                message_id = get_w3c_message_id(parse_params.headers)
        return {
            "id": str(uuid.uuid4()),
            "type": HTTP_TYPE,
            "info": {
                "httpInfo": {
                    "host": parse_params.host if parse_params else "",
                    "request": additional_info,
                },
                **({"messageId": message_id} if message_id else {}),
            },
            "started": get_current_ms_time(),
        }

    def parse_response(self, url: str, status_code: int, headers: dict, body: bytes) -> dict:  # type: ignore[type-arg]
        max_size = CoreConfiguration.get_max_entry_size(has_error=is_error_code(status_code))
        if Configuration.verbose and not should_scrub_domain(url):
            additional_info = {
                "headers": lumigo_dumps_with_context("responseHeaders", headers, max_size),
                "body": lumigo_dumps_with_context("responseBody", body, max_size)
                if body and not Configuration.skip_collecting_http_body
                else "",
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

    @staticmethod
    def scrub_query_params(uri: str) -> str:
        with lumigo_safe_execute("scrub_query_params"):
            regexes = (
                CoreConfiguration.secret_masking_regex_http_query_params or get_omitting_regex()
            )
            if not uri or "?" not in uri or not regexes:
                return uri
            parsed_url = urlparse(uri)
            parsed_url = parsed_url._replace(
                query=urlencode(
                    [
                        (key, "----" if regexes.match(key) else value)
                        for key, value in parse_qsl(parsed_url.query)
                    ]
                )
            )
            return urlunparse(parsed_url)


class ServerlessAWSParser(Parser):
    # Override this field to add message id using the amz headers
    should_add_message_id = True

    def parse_response(self, url: str, status_code: int, headers: dict, body: bytes) -> dict:  # type: ignore[type-arg]
        additional_info = {}
        message_id = headers.get("x-amzn-requestid")
        if message_id and self.should_add_message_id:
            additional_info["info"] = {"messageId": message_id}
        span_id = headers.get("x-amzn-requestid") or headers.get("x-amz-requestid")
        if span_id:
            additional_info["id"] = span_id
        return recursive_json_join(  # type: ignore[no-any-return]
            additional_info, super().parse_response(url, status_code, headers, body)
        )


class DynamoParser(ServerlessAWSParser):
    should_add_message_id = False

    @staticmethod
    def _extract_message_id(body: dict, method: str) -> Optional[str]:  # type: ignore[type-arg]
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
    def _extract_table_name(body: dict, method: str) -> Optional[str]:  # type: ignore[type-arg]
        name = body.get("TableName")
        if not name and method == "BatchWriteItem" and isinstance(body.get("RequestItems"), dict):
            return next(iter(body["RequestItems"]))  # type: ignore[no-any-return]
        return name

    def parse_request(self, parse_params: HttpRequest) -> dict:  # type: ignore[type-arg]
        target: str = parse_params.headers.get("x-amz-target", "")
        method = safe_split_get(target, ".", 1)
        try:
            parsed_body = json.loads(parse_params.body)
        except json.JSONDecodeError as e:
            get_logger().debug("Error while trying to parse ddb request body", exc_info=e)
            parsed_body = {}

        return recursive_json_join(  # type: ignore[no-any-return]
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
    def parse_request(self, parse_params: HttpRequest) -> dict:  # type: ignore[type-arg]
        arn = safe_key_from_query(parse_params.body, "TopicArn") or safe_key_from_query(
            parse_params.body, "TargetArn"
        )
        return recursive_json_join(  # type: ignore[no-any-return]
            {
                "info": {
                    "resourceName": arn,
                    "targetArn": arn,
                }
            },
            super().parse_request(parse_params),
        )

    def parse_response(
        self, url: str, status_code: int, headers: Dict[str, Any], body: bytes
    ) -> dict:  # type: ignore[type-arg]
        return recursive_json_join(  # type: ignore[no-any-return]
            {
                "info": {
                    "messageId": safe_key_from_xml(body, "PublishResponse/PublishResult/MessageId")
                }
            },
            super().parse_response(url, status_code, headers, body),
        )


class LambdaParser(ServerlessAWSParser):
    def parse_request(self, parse_params: HttpRequest) -> dict:  # type: ignore[type-arg]
        decoded_uri = safe_split_get(unquote(parse_params.uri), "/", 3)
        return recursive_json_join(  # type: ignore[no-any-return]
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
    def parse_request(self, parse_params: HttpRequest) -> dict:  # type: ignore[type-arg]
        return recursive_json_join(  # type: ignore[no-any-return]
            {"info": {"resourceName": safe_key_from_json(parse_params.body, "StreamName")}},
            super().parse_request(parse_params),
        )

    def parse_response(
        self, url: str, status_code: int, headers: Dict[str, Any], body: bytes
    ) -> dict:  # type: ignore[type-arg]
        return recursive_json_join(  # type: ignore[no-any-return]
            {"info": {"messageId": KinesisParser._extract_message_id(body)}},
            super().parse_response(url, status_code, headers, body),
        )

    @staticmethod
    def _extract_message_id(response_body: bytes) -> Optional[str]:
        return safe_key_from_json(response_body, "SequenceNumber") or safe_get(  # type: ignore[return-value]
            safe_key_from_json(response_body, "Records", []), [0, "SequenceNumber"]  # type: ignore[arg-type]
        )

    @staticmethod
    def get_omit_skip_path() -> Optional[List[str]]:
        return ["PartitionKey"]


class SqsXmlParser(ServerlessAWSParser):
    def parse_request(self, parse_params: HttpRequest) -> dict:  # type: ignore[type-arg]
        return recursive_json_join(  # type: ignore[no-any-return]
            {"info": {"resourceName": self._extract_queue_url(parse_params.body)}},
            super().parse_request(parse_params),
        )

    def parse_response(
        self, url: str, status_code: int, headers: Dict[str, Any], body: bytes
    ) -> dict:  # type: ignore[type-arg]
        return recursive_json_join(  # type: ignore[no-any-return]
            {"info": {"messageId": self._extract_message_id(body)}},
            super().parse_response(url, status_code, headers, body),
        )

    @staticmethod
    def _extract_message_id(response_body: bytes) -> Optional[str]:
        return (  # type: ignore[no-any-return]
            safe_key_from_xml(
                response_body, "SendMessageResponse/SendMessageResult/MessageId"  # Single.
            )
            or safe_key_from_xml(  # noqa: W503
                response_body,
                "SendMessageBatchResponse/SendMessageBatchResult/SendMessageBatchResultEntry/0/MessageId",  # Batch.
            )
            or safe_key_from_xml(  # noqa: W503
                response_body,
                "SendMessageBatchResponse/SendMessageBatchResult/SendMessageBatchResultEntry/MessageId",  # Batch.
            )
        )

    @staticmethod
    def _extract_queue_url(request_body: bytes) -> Optional[str]:
        return safe_key_from_query(request_body, "QueueUrl")


class SqsJsonParser(ServerlessAWSParser):
    def parse_request(self, parse_params: HttpRequest) -> dict:  # type: ignore[type-arg]
        return recursive_json_join(  # type: ignore[no-any-return]
            {"info": {"resourceName": self._extract_queue_url(parse_params.body)}},
            super().parse_request(parse_params),
        )

    def parse_response(
        self, url: str, status_code: int, headers: Dict[str, Any], body: bytes
    ) -> dict:  # type: ignore[type-arg]
        return recursive_json_join(  # type: ignore[no-any-return]
            {"info": {"messageId": self._extract_message_id(body)}},
            super().parse_response(url, status_code, headers, body),
        )

    @staticmethod
    def _extract_message_id(response_body: bytes) -> Optional[str]:
        parsed_body = {}
        try:
            parsed_body = json.loads(response_body)
        except json.JSONDecodeError as e:
            get_logger().warning("Error while trying to parse sqs json request body", exc_info=e)

        # If the request was to send a single message this should work
        message_id = parsed_body.get("MessageId")
        if message_id and isinstance(message_id, str):
            return message_id

        # This should work if the request was to send a batch of messages.
        messages = parsed_body.get("Successful", [])
        if (
            messages
            and isinstance(messages[0], dict)
            and messages[0].get("MessageId")
            and isinstance(messages[0].get("MessageId"), str)
        ):
            return messages[0].get("MessageId")

        get_logger().warning("No MessageId was found in the SQS response body")

        return None

    @staticmethod
    def _extract_queue_url(request_body: bytes) -> Optional[str]:
        queue_url = safe_key_from_json(json_str=request_body, key="QueueUrl", default=None)
        return queue_url if isinstance(queue_url, str) else None


class S3Parser(Parser):
    def parse_request(self, parse_params: HttpRequest) -> dict:  # type: ignore[type-arg]
        resource_name = safe_split_get(parse_params.host, ".", 0)
        if resource_name == "s3":
            resource_name = safe_split_get(parse_params.uri, "/", 1)
        return recursive_json_join(  # type: ignore[no-any-return]
            {"info": {"resourceName": resource_name}},
            super().parse_request(parse_params),
        )

    def parse_response(
        self, url: str, status_code: int, headers: Dict[str, Any], body: bytes
    ) -> dict:  # type: ignore[type-arg]
        return recursive_json_join(  # type: ignore[no-any-return]
            {"info": {"messageId": headers.get("x-amz-request-id")}},
            super().parse_response(url, status_code, headers, body),
        )


class EventBridgeParser(Parser):
    def parse_request(self, parse_params: HttpRequest) -> dict:  # type: ignore[type-arg]
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
        return recursive_json_join(  # type: ignore[no-any-return]
            {"info": {"resourceNames": list(resource_names) or None}},
            super().parse_request(parse_params),
        )

    def parse_response(
        self, url: str, status_code: int, headers: Dict[str, Any], body: bytes
    ) -> dict:  # type: ignore[type-arg]
        try:
            parsed_body = json.loads(body)
        except json.JSONDecodeError as e:
            get_logger().debug("Error while trying to parse eventBridge response body", exc_info=e)
            parsed_body = {}
        message_ids = []
        if isinstance(parsed_body.get("Entries"), list):
            message_ids = [e["EventId"] for e in parsed_body["Entries"] if e.get("EventId")]
        return recursive_json_join(  # type: ignore[no-any-return]
            {"info": {"messageIds": message_ids}},
            super().parse_response(url, status_code, headers, body),
        )


class ApiGatewayV2Parser(ServerlessAWSParser):
    # API-GW V1 covered by ServerlessAWSParser

    def parse_response(
        self, url: str, status_code: int, headers: Dict[str, Any], body: bytes
    ) -> dict:  # type: ignore[type-arg]
        aws_request_id = headers.get("x-amzn-requestid")
        apigw_request_id = headers.get("apigw-requestid")
        message_id = aws_request_id or apigw_request_id
        return recursive_json_join(  # type: ignore[no-any-return]
            {"info": {"messageId": message_id}},
            super().parse_response(url, status_code, headers, body),
        )


def get_parser(host: str, headers: Optional[dict] = None) -> Type[Parser]:  # type: ignore[type-arg]
    """
    Returns the matching Parser class based on the given http request properties
    @param host: The http host the request was sent to (i.e. "google.com", without "https://")
    @param headers: The http headers sent with the request, with all keys being lowercase
    @return: Parser class best matching to parse the given http request
    """

    _headers = headers if headers else {}

    if should_use_tracer_extension():
        return Parser
    if "amazonaws.com" not in host and not _headers.get("x-amzn-requestid"):
        return Parser
    service = safe_split_get(host, ".", 0)
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
    elif safe_split_get(host, ".", 1) == "s3" or safe_split_get(host, ".", 0) == "s3":
        return S3Parser
    # SQS Legacy Endpoints: https://docs.aws.amazon.com/general/latest/gr/rande.html
    elif service in ("sqs", "sqs-fips") or "queue.amazonaws.com" in host:
        using_json_protocol = (
            _headers.get("content-type", "").lower().startswith("application/x-amz-json-")
        )
        return SqsJsonParser if using_json_protocol else SqsXmlParser
    elif "execute-api" in host:
        return ApiGatewayV2Parser
    return ServerlessAWSParser
