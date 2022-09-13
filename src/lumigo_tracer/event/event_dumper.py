import copy
import os
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Dict, List, Optional

from lumigo_tracer.parsing_utils import str_to_list, safe_get
from lumigo_tracer.lumigo_utils import (
    get_logger,
    is_api_gw_event,
    lumigo_dumps,
    Configuration,
    should_use_tracer_extension,
    aws_dump,
)

EVENT_MAX_SIZE = 6 * 1024 * 1024

API_GW_KEYS_ORDER = str_to_list(os.environ.get("LUMIGO_API_GW_KEYS_ORDER", "")) or [
    "version",
    "routeKey",
    "rawPath",
    "rawQueryString",
    "resource",
    "path",
    "httpMethod",
    "queryStringParameters",
    "pathParameters",
    "body",
    "requestContext",
    "headers",
]

CLOUDFRONT_KEYS_ORDER = str_to_list(os.environ.get("LUMIGO_CLOUDFRONT_KEYS_ORDER", "")) or [
    "config"
]

CLOUDFRONT_REQUEST_KEYS_ORDER = str_to_list(
    os.environ.get("LUMIGO_CLOUDFRONT_REQUEST_KEYS_ORDER", "")
) or ["body", "clientIp", "method", "querystring", "uri"]

S3_KEYS_ORDER = str_to_list(os.environ.get("LUMIGO_S3_KEYS_ORDER", "")) or [
    "awsRegion",
    "eventTime",
    "eventName",
    "userIdentity",
    "requestParameters",
]

S3_BUCKET_KEYS_ORDER = str_to_list(os.environ.get("LUMIGO_S3_BUCKET_KEYS_ORDER", "")) or ["arn"]

S3_OBJECT_KEYS_ORDER = str_to_list(os.environ.get("LUMIGO_S3_OBJECT_KEYS_ORDER", "")) or [
    "key",
    "size",
]

API_GW_PREFIX_KEYS_HEADERS_DELETE_KEYS = str_to_list(
    os.environ.get("LUMIGO_API_GW_PREFIX_KEYS_HEADERS_DELETE_KEYS", "")
) or ("cookie", "x-amz", "accept", "cloudfront", "via", "x-forwarded", "sec-")
API_GW_REQUEST_CONTEXT_FILTER_KEYS = str_to_list(
    os.environ.get("LUMIGO_API_GW_REQUEST_CONTEXT_FILTER_KEYS", "")
) or ["authorizer", "http", "requestid"]
API_GW_KEYS_DELETE_KEYS = str_to_list(os.environ.get("LUMIGO_API_GW_KEYS_DELETE_KEYS", "")) or [
    "multiValueHeaders",
    "multiValueQueryStringParameters",
]

SNS_KEYS_ORDER = str_to_list(os.environ.get("LUMIGO_SNS_KEYS_ORDER", "")) or [
    "Message",
    "MessageAttributes",
    "MessageId",
]

SQS_KEYS_ORDER = str_to_list(os.environ.get("LUMIGO_SQS_KEYS_ORDER", "")) or [
    "body",
    "messageAttributes",
    "messageId",
]


class Event:
    def __init__(self, event):  # type: ignore[no-untyped-def]
        """
        Cache propeties of the event in order improve performance.
        """
        self.raw_event = event
        self.record_event_source = safe_get(event, ["Records", 0, "eventSource"])


class EventParseHandler(ABC):
    @staticmethod
    @abstractmethod
    def is_supported(event: Event) -> bool:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def parse(event) -> OrderedDict:  # type: ignore[no-untyped-def,type-arg]
        raise NotImplementedError()

    @staticmethod
    def get_omit_skip_path() -> Optional[List[str]]:
        return None


class S3Handler(EventParseHandler):
    @staticmethod
    def is_supported(event: Event) -> bool:
        return event.record_event_source == "aws:s3"  # type: ignore[no-any-return]

    @staticmethod
    def parse(event) -> OrderedDict:  # type: ignore[no-untyped-def,type-arg]
        new_event: OrderedDict = OrderedDict()  # type: ignore[type-arg]
        new_event["Records"] = []

        for rec in event.get("Records", []):
            new_s3_record_event: OrderedDict = OrderedDict()  # type: ignore[type-arg]
            for key in S3_KEYS_ORDER:
                if rec.get(key) is not None:
                    new_s3_record_event[key] = rec.get(key)
            if rec.get("s3"):
                new_s3_record_event["s3"] = {}
                if rec["s3"].get("bucket") is not None:
                    new_s3_record_event["s3"]["bucket"] = {}
                    for key in S3_BUCKET_KEYS_ORDER:
                        new_s3_record_event["s3"]["bucket"][key] = rec["s3"]["bucket"].get(key)
                if rec["s3"].get("object") is not None:
                    new_s3_record_event["s3"]["object"] = {}
                    for key in S3_OBJECT_KEYS_ORDER:
                        new_s3_record_event["s3"]["object"][key] = rec["s3"]["object"].get(key)
            new_event["Records"].append(new_s3_record_event)
        return new_event

    @staticmethod
    def get_omit_skip_path() -> Optional[List[str]]:
        return ["Records", "s3", "object", "key"]


class CloudfrontHandler(EventParseHandler):
    @staticmethod
    def is_supported(event: Event) -> bool:
        return bool(safe_get(event.raw_event, ["Records", 0, "cf", "config", "distributionId"], {}))

    @staticmethod
    def parse(event) -> OrderedDict:  # type: ignore[no-untyped-def,type-arg]
        new_event: OrderedDict = OrderedDict()  # type: ignore[type-arg]
        new_event["Records"] = []

        for rec in event.get("Records", []):
            cf_record = rec.get("cf", {})
            new_cloudfront_record_event: OrderedDict = OrderedDict()  # type: ignore[type-arg]
            new_cloudfront_record_event["cf"] = {}
            for key in CLOUDFRONT_KEYS_ORDER:
                if cf_record.get(key):
                    new_cloudfront_record_event["cf"][key] = cf_record.get(key)
            if cf_record.get("request") is not None:
                new_cloudfront_record_event["cf"]["request"] = {}
                for key in CLOUDFRONT_REQUEST_KEYS_ORDER:
                    if cf_record["request"].get(key) is not None:
                        new_cloudfront_record_event["cf"]["request"][key] = cf_record[
                            "request"
                        ].get(key)
            new_event["Records"].append(new_cloudfront_record_event)
        return new_event


class ApiGWHandler(EventParseHandler):
    @staticmethod
    def is_supported(event: Event) -> bool:
        return is_api_gw_event(event=event.raw_event)

    @staticmethod
    def parse(event) -> OrderedDict:  # type: ignore[no-untyped-def,type-arg]
        new_event: OrderedDict = OrderedDict()  # type: ignore[type-arg]
        # Add order keys
        for order_key in API_GW_KEYS_ORDER:
            if event.get(order_key):
                new_event[order_key] = copy.deepcopy(event[order_key])
        # Remove requestContext keys
        if new_event.get("requestContext"):
            for rc_key in new_event["requestContext"].copy():
                if rc_key.lower() not in API_GW_REQUEST_CONTEXT_FILTER_KEYS:
                    new_event["requestContext"].pop(rc_key, None)
        # Remove headers keys
        if new_event.get("headers"):
            for h_key in new_event["headers"].copy():
                if any(h_key.lower().startswith(s) for s in API_GW_PREFIX_KEYS_HEADERS_DELETE_KEYS):
                    new_event["headers"].pop(h_key, None)
        # Add all other keys
        for key in event.keys():
            if (key not in API_GW_KEYS_ORDER) and (key not in API_GW_KEYS_DELETE_KEYS):
                new_event[key] = copy.deepcopy(event[key])
        return new_event


class SNSHandler(EventParseHandler):
    @staticmethod
    def is_supported(event: Event) -> bool:
        return safe_get(event.raw_event, ["Records", 0, "EventSource"]) == "aws:sns"  # type: ignore[no-any-return]

    @staticmethod
    def parse(event) -> OrderedDict:  # type: ignore[no-untyped-def,type-arg]
        new_sns_event: OrderedDict = OrderedDict()  # type: ignore[type-arg]
        new_sns_event["Records"] = []
        # Add order keys
        for rec in event.get("Records"):
            new_sns_record_event: OrderedDict = OrderedDict()  # type: ignore[type-arg]
            for key in SNS_KEYS_ORDER:
                if rec["Sns"].get(key):
                    new_sns_record_event[key] = rec["Sns"].get(key)
            new_sns_event["Records"].append({"Sns": new_sns_record_event})
        return new_sns_event


class SQSHandler(EventParseHandler):
    @staticmethod
    def is_supported(event: Event) -> bool:
        return event.record_event_source == "aws:sqs"  # type: ignore[no-any-return]

    @staticmethod
    def parse(event) -> OrderedDict:  # type: ignore[no-untyped-def,type-arg]
        new_sqs_event: OrderedDict = OrderedDict()  # type: ignore[type-arg]
        new_sqs_event["Records"] = []
        # Add order keys
        for rec in event.get("Records"):
            new_sqs_record_event: OrderedDict = OrderedDict()  # type: ignore[type-arg]
            for key in SQS_KEYS_ORDER:
                if rec.get(key):
                    new_sqs_record_event[key] = rec.get(key)
            new_sqs_event["Records"].append(new_sqs_record_event)
        return new_sqs_event


class DDBHandler(EventParseHandler):
    @staticmethod
    def is_supported(event: Event) -> bool:
        return event.record_event_source == "aws:dynamodb"  # type: ignore[no-any-return]

    @staticmethod
    def parse(event) -> OrderedDict:  # type: ignore[no-untyped-def,type-arg]
        return event  # type: ignore[no-any-return]

    @staticmethod
    def get_omit_skip_path() -> Optional[List[str]]:
        return ["Records", "dynamodb", "Keys"]


class EventDumper:
    @staticmethod
    def dump_event(
        event: Dict, handlers: List[EventParseHandler] = None, has_error: bool = False  # type: ignore[type-arg]
    ) -> str:
        max_size = Configuration.get_max_entry_size(has_error)
        if should_use_tracer_extension():
            return aws_dump(event)
        handlers = handlers or [
            ApiGWHandler(),
            SNSHandler(),
            SQSHandler(),
            S3Handler(),
            CloudfrontHandler(),
            DDBHandler(),
        ]
        event_obj = Event(event)
        for handler in handlers:
            try:
                if handler.is_supported(event_obj):
                    return lumigo_dumps(
                        handler.parse(event),
                        max_size,
                        omit_skip_path=handler.get_omit_skip_path(),
                    )
            except Exception as e:
                get_logger().debug(
                    f"Error while trying to parse with handler {handler.__class__.__name__} event {event}",
                    exc_info=e,
                )
        return lumigo_dumps(event, max_size)
