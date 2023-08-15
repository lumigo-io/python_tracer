import random
import re
from typing import Dict, Optional

from lumigo_core.logger import get_logger

TRACEPARENT_HEADER_NAME = "traceparent"
TRACESTATE_HEADER_NAME = "tracestate"
# The regex was copied from:
# https://github.com/open-telemetry/opentelemetry-python/blob/cad776a2031c84fb3c3a1af90ee2a939f3394b9a/opentelemetry-api/src/opentelemetry/trace/propagation/tracecontext.py#L28
TRACEPARENT_HEADER_FORMAT = (
    "^[ \t]*([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})" + "(-.*)?[ \t]*$"
)
TRACEPARENT_HEADER_FORMAT_RE = re.compile(TRACEPARENT_HEADER_FORMAT)
SKIP_INJECT_HEADERS = ["x-amz-content-sha256"]


def generate_message_id() -> str:
    return "%016x" % random.getrandbits(64)


def should_skip_trace_propagation(headers: Dict[str, str]) -> bool:
    return any(key.lower() in SKIP_INJECT_HEADERS for key in headers)


def add_w3c_trace_propagator(headers: Dict[str, str], transaction_id: str) -> None:
    if should_skip_trace_propagation(headers):
        get_logger().debug("Skipping trace propagation")
        return
    message_id = generate_message_id()
    headers[TRACEPARENT_HEADER_NAME] = get_trace_id(headers, transaction_id, message_id)
    headers[TRACESTATE_HEADER_NAME] = get_trace_state(headers, message_id)


def get_trace_id(headers: Dict[str, str], transaction_id: str, message_id: str) -> str:
    version = None
    trace_id = None
    span_id = None
    trace_flags = None
    match = re.search(TRACEPARENT_HEADER_FORMAT_RE, headers.get(TRACEPARENT_HEADER_NAME) or "")
    if match:
        version = match.group(1)
        trace_id = match.group(2)
        span_id = match.group(3)
        trace_flags = match.group(4)
    if not match or trace_id == "0" * 32 or span_id == "0" * 16 or version == "ff":
        version = "00"  # constant
        trace_id = transaction_id.ljust(32, "0")  # random 32 characters long
        trace_flags = "01"  # don't ignore the span
    span_id = message_id  # random 16 characters long - this is the message id
    return f"{version}-{trace_id}-{span_id}-{trace_flags}"


def get_trace_state(headers: Dict[str, str], message_id: str) -> str:
    lumigo_state = f"lumigo={message_id}"
    if headers.get(TRACESTATE_HEADER_NAME):
        return headers[TRACESTATE_HEADER_NAME] + f",{lumigo_state}"
    return lumigo_state


def is_w3c_headers(headers: Dict[str, str]) -> bool:
    return bool(re.search(TRACEPARENT_HEADER_FORMAT_RE, headers.get(TRACEPARENT_HEADER_NAME) or ""))


def get_w3c_message_id(headers: Dict[str, str]) -> Optional[str]:
    match = re.search(TRACEPARENT_HEADER_FORMAT_RE, headers.get(TRACEPARENT_HEADER_NAME) or "")
    if match:
        return match.group(3)
    return None
