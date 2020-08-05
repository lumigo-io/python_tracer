import copy
import os
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Dict, List

from lumigo_tracer.parsers.utils import str_to_list, safe_get
from lumigo_tracer.utils import get_logger, is_api_gw_event

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
API_GW_PREFIX_KEYS_HEADERS_DELETE_KEYS = str_to_list(
    os.environ.get("LUMIGO_API_GW_PREFIX_KEYS_HEADERS_DELETE_KEYS", "")
) or ("cookie", "x-amz", "accept", "cloudfront", "via", "x-forwarded", "sec-")
API_GW_REQUEST_CONTEXT_FILTER_KEYS = str_to_list(
    os.environ.get("LUMIGO_API_GW_REQUEST_CONTEXT_FILTER_KEYS", "")
) or ["authorizer", "http"]
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


class EventParseHandler(ABC):
    @staticmethod
    @abstractmethod
    def is_supported(event) -> bool:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def parse(event) -> Dict:
        raise NotImplementedError()


class ApiGWHandler(EventParseHandler):
    @staticmethod
    def is_supported(event) -> bool:
        if is_api_gw_event(event=event):  # noqa
            return True
        return False

    @staticmethod
    def parse(event) -> Dict:
        new_event: OrderedDict = OrderedDict()
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
    def is_supported(event) -> bool:
        return safe_get(event, ["Records", 0, "EventSource"]) == "aws:sns"

    @staticmethod
    def parse(event) -> Dict:
        new_sns_event: OrderedDict = OrderedDict()
        new_sns_event["Records"] = []
        # Add order keys
        for rec in event.get("Records"):
            new_sns_record_event: OrderedDict = OrderedDict()
            for key in SNS_KEYS_ORDER:
                if rec["Sns"].get(key):
                    new_sns_record_event[key] = rec["Sns"].get(key)
            new_sns_event["Records"].append({"Sns": new_sns_record_event})
        return new_sns_event


class SQSHandler(EventParseHandler):
    @staticmethod
    def is_supported(event) -> bool:
        return safe_get(event, ["Records", 0, "eventSource"]) == "aws:sqs"

    @staticmethod
    def parse(event) -> Dict:
        new_sqs_event: OrderedDict = OrderedDict()
        new_sqs_event["Records"] = []
        # Add order keys
        for rec in event.get("Records"):
            new_sqs_record_event: OrderedDict = OrderedDict()
            for key in SQS_KEYS_ORDER:
                if rec.get(key):
                    new_sqs_record_event[key] = rec.get(key)
            new_sqs_event["Records"].append(new_sqs_record_event)
        return new_sqs_event


class EventParser:
    @staticmethod
    def parse_event(event: Dict, handlers: List[EventParseHandler] = None):
        handlers = handlers or [ApiGWHandler(), SNSHandler(), SQSHandler()]
        for handler in handlers:
            try:
                if handler.is_supported(event):
                    return handler.parse(event)
            except Exception as e:
                get_logger().debug(
                    f"Error while trying to parse with handler {handler.__class__.__name__} event {event}",
                    exc_info=e,
                )
        return event
