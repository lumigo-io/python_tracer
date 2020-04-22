import re
from collections import OrderedDict
from typing import Dict

API_GW_REGEX = re.compile(rf".*execute-api.*amazonaws\.com.*")
API_GW_KEYS_ORDER = [
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

API_GW_PREFIX_KEYS_HEADERS_DELETE_KEYS = (
    "cookie",
    "X-Amz",
    "Accept",
    "CloudFront",
    "Via",
    "X-Forwarded",
    "sec-",
)

API_GW_REQUEST_CONTEXT_FILTER_KEYS = ["authorizer"]

API_GW_KEYS_DELETE_KEYS = ["multiValueHeaders"]


def parse_event(event: Dict):
    try:
        if _is_api_gw_event(event):
            return _parse_api_gw_event(event)
        else:
            return event
    except Exception:
        return event


def _is_api_gw_event(event: Dict) -> bool:
    return API_GW_REGEX.match(event["requestContext"]["domainName"]) is not None


def _parse_api_gw_event(event: Dict):
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
