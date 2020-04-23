import os
import re
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Dict, List

from lumigo_tracer.parsers.utils import str_to_list
from lumigo_tracer.utils import get_logger


API_GW_REGEX = re.compile(r".*execute-api.*amazonaws\.com.*")
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
    "multiValueHeaders"
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
        if event.get("requestContext") and event.get("requestContext", {}).get("domainName"):
            return API_GW_REGEX.match(event["requestContext"]["domainName"]) is not None
        return False

    @staticmethod
    def parse(event) -> Dict:
        new_event: OrderedDict = OrderedDict()
        # Add order keys
        for order_key in API_GW_KEYS_ORDER:
            if event.get(order_key):
                new_event[order_key] = event[order_key]
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
                new_event[key] = event[key]
        return new_event


class EventParser:
    @staticmethod
    def parse_event(event: Dict, handlers: List[EventParseHandler] = None):
        handlers = handlers or [ApiGWHandler()]
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
