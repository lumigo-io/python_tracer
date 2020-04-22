import os
import re
from collections import OrderedDict
from typing import Dict, List

from lumigo_tracer.parsers.utils import str_to_tuple, str_to_list
from lumigo_tracer.utils import get_logger


API_GW_REGEX = re.compile(r".*execute-api.*amazonaws\.com.*")
API_GW_KEYS_ORDER = str_to_list(os.environ.get("LUMIGO_API_GW_KEYS_ORDER", "")) or [
    "resource",
    "path",
    "httpMethod",
    "queryStringParameters",
    "multiValueQueryStringParameters",
    "pathParameters",
    "body",
    "requestContext",
    "headers",
]
API_GW_PREFIX_KEYS_HEADERS_DELETE_KEYS = str_to_tuple(
    os.environ.get("LUMIGO_API_GW_PREFIX_KEYS_HEADERS_DELETE_KEYS", "")
) or ("cookie", "X-Amz", "Accept", "CloudFront", "Via", "X-Forwarded", "sec-")
API_GW_REQUEST_CONTEXT_FILTER_KEYS = str_to_list(
    os.environ.get("LUMIGO_API_GW_REQUEST_CONTEXT_FILTER_KEYS", "")
) or ["authorizer"]
API_GW_KEYS_DELETE_KEYS = str_to_list(os.environ.get("LUMIGO_API_GW_KEYS_DELETE_KEYS", "")) or [
    "multiValueHeaders"
]


class EventParseHandler:
    def is_supported(self, event) -> bool:
        raise NotImplementedError()

    def parse(self, event):
        raise NotImplementedError()


class ApiGWHandler(EventParseHandler):
    def is_supported(self, event) -> bool:
        if event.get("requestContext") and event.get("requestContext").get("domainName"):
            return API_GW_REGEX.match(event["requestContext"]["domainName"]) is not None
        return False

    def parse(self, event):
        new_event: OrderedDict = OrderedDict()
        # Add order keys
        for order_key in API_GW_KEYS_ORDER:
            if event.get(order_key):
                new_event[order_key] = event[order_key]
        # Remove requestContext keys
        if new_event.get("requestContext"):
            delete_request_context_keys = [
                x
                for x in new_event["requestContext"].keys()
                if x not in API_GW_REQUEST_CONTEXT_FILTER_KEYS
            ]
            for delete_request_context_key in delete_request_context_keys:
                new_event["requestContext"].pop(delete_request_context_key, None)
        # Remove headers keys
        if new_event.get("headers"):
            delete_headers_keys = [
                x
                for x in new_event["headers"].keys()
                if x.startswith(API_GW_PREFIX_KEYS_HEADERS_DELETE_KEYS)
            ]
            for delete_headers_key in delete_headers_keys:
                new_event["headers"].pop(delete_headers_key, None)
        # Add all other keys
        for key in event.keys():
            if (key not in API_GW_KEYS_ORDER) and (key not in API_GW_KEYS_DELETE_KEYS):
                new_event[key] = event[key]
        return new_event


HANDLERS: List[EventParseHandler] = [ApiGWHandler()]


class EventParser:
    @staticmethod
    def parse_event(event: Dict, handlers: List[EventParseHandler] = HANDLERS):
        for handler in handlers:
            try:
                if handler.is_supported(event):
                    return handler.parse(event)
            except Exception as e:
                get_logger().debug(
                    f"Error while trying to parse with handler {type(handler)} event {event}",
                    exc_info=e,
                )
        return event
