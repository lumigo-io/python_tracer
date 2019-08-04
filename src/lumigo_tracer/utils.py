import json
import logging
import os
import time
import urllib.request
from urllib.error import URLError
from typing import Union, List
from contextlib import contextmanager
from math import ceil


EDGE_HOST = "https://{region}.lumigo-tracer-edge.golumigo.com/api/spans"
LOG_FORMAT = "#LUMIGO# - %(asctime)s - %(levelname)s - %(message)s"
_SHOULD_REPORT = True
SECONDS_TO_TIMEOUT = 0.3
MAX_SIZE_FOR_REQUEST: int = int(os.environ.get("MAX_SIZE_FOR_REQUEST", 900_000))

_HOST: str = ""
_TOKEN: str = ""
_VERBOSE: bool = True
_ENHANCE_PRINT: bool = False
_logger: Union[logging.Logger, None] = None


def config(
    edge_host: str = "",
    should_report: Union[bool, None] = None,
    token: str = None,
    verbose: bool = True,
    enhance_print: bool = False,
) -> None:
    """
    This function configure the lumigo wrapper.

    :param verbose: Whether the tracer should send all the possible information (debug mode)
    :param edge_host: The host to send the events. Leave empty for default.
    :param should_report: Weather we should send the events. Change to True in the production.
    :param token: The token to use when sending back the events.
    """
    global _HOST, _SHOULD_REPORT, _TOKEN, _VERBOSE, _ENHANCE_PRINT
    if edge_host:
        _HOST = edge_host
    elif os.environ.get("LUMIGO_TRACER_HOST"):
        _HOST = os.environ["LUMIGO_TRACER_HOST"]
    if should_report is not None:
        _SHOULD_REPORT = should_report
    elif not is_aws_environment():
        _SHOULD_REPORT = False
    if token:
        _TOKEN = token
    elif os.environ.get("LUMIGO_TRACER_TOKEN"):
        _TOKEN = os.environ["LUMIGO_TRACER_TOKEN"]
    _ENHANCE_PRINT = enhance_print
    if not verbose or os.environ.get("LUMIGO_VERBOSE", "").lower() == "false":
        _VERBOSE = False
    else:
        _VERBOSE = True


def _is_span_has_error(span: dict) -> bool:
    return (
        span.get("error") is not None
        or span.get("info", {}).get("httpInfo", {}).get("response", {}).get("statusCode", 0)  # noqa
        > 400  # noqa
    )


def _get_event_base64_size(event) -> int:
    return ceil(len(json.dumps(event)) * 4 / 3)


def _create_request_body(
    msgs: List[dict], prune_size_flag: bool, max_size: int = MAX_SIZE_FOR_REQUEST
) -> str:

    if not prune_size_flag or _get_event_base64_size(msgs) < max_size:
        return json.dumps(msgs)

    end_span = msgs[-1]
    error_spans = [span for span in msgs if _is_span_has_error(span) and span != end_span]
    normal_spans = [span for span in msgs if not _is_span_has_error(span) and span != end_span]
    ordered_spans = []
    ordered_spans.extend(error_spans)
    ordered_spans.extend(normal_spans)

    spans_to_send: list = []
    for span in ordered_spans:
        current_size = _get_event_base64_size(spans_to_send) + _get_event_base64_size(end_span)
        span_size = _get_event_base64_size(span)
        if current_size + span_size < max_size:
            spans_to_send.append(span)
    spans_to_send.append(end_span)
    return json.dumps(spans_to_send)


def report_json(region: Union[None, str], msgs: List[dict]) -> int:
    """
    This function sends the information back to the edge.

    :param region: The region to use as default if not configured otherwise.
    :param msgs: the message to send.
    :return: The duration of reporting (in milliseconds),
                or 0 if we didn't send (due to configuration or fail).
    """
    for msg in msgs:
        msg["token"] = _TOKEN
    get_logger().info(f"reporting the messages: {msgs}")
    host = _HOST or EDGE_HOST.format(region=region)
    duration = 0
    if _SHOULD_REPORT:
        try:
            prune_trace: bool = not os.environ.get("LUMIGO_PRUNE_TRACE_OFF", False) == "TRUE"
            to_send = _create_request_body(msgs, prune_trace).encode()
            start_time = time.time()
            response = urllib.request.urlopen(
                urllib.request.Request(host, to_send, headers={"Content-Type": "application/json"}),
                timeout=float(os.environ.get("LUMIGO_EDGE_TIMEOUT", SECONDS_TO_TIMEOUT)),
            )
            duration = int((time.time() - start_time) * 1000)
            get_logger().info(f"successful reporting, code: {getattr(response, 'code', 'unknown')}")
        except URLError as e:
            get_logger().exception(f"Timeout when reporting to {host}", exc_info=e)
        except Exception as e:
            get_logger().exception(f"could not report json to {host}", exc_info=e)
    return duration


def get_logger():
    """
    This function returns lumigo's logger.
    The logger streams the logs to the stderr in format the explicitly say that those are lumigo's logs.

    This logger is off by default.
    Add the environment variable `LUMIGO_DEBUG=true` to activate it.
    """
    global _logger
    if not _logger:
        _logger = logging.getLogger("lumigo")
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        if os.environ.get("LUMIGO_DEBUG", "").lower() == "true":
            _logger.setLevel(logging.DEBUG)
        else:
            _logger.setLevel(logging.CRITICAL)
        _logger.addHandler(handler)
    return _logger


def is_verbose():
    return _VERBOSE


@contextmanager
def lumigo_safe_execute(part_name=""):
    try:
        yield
    except Exception as e:
        get_logger().exception(f"An exception occurred in lumigo's code {part_name}", exc_info=e)


def is_aws_environment():
    """
    :return: heuristically determine rather we're running on an aws environment.
    """
    return bool(os.environ.get("LAMBDA_RUNTIME_DIR"))


def is_enhanced_print():
    return _ENHANCE_PRINT
