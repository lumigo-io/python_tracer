import json
import logging
import os
import time
import urllib.request
from urllib.error import URLError
from typing import Union, List, Optional, Dict, Any
from contextlib import contextmanager
from base64 import b64encode
import inspect

EDGE_HOST = "https://{region}.lumigo-tracer-edge.golumigo.com/api/spans"
LOG_FORMAT = "#LUMIGO# - %(asctime)s - %(levelname)s - %(message)s"
SECONDS_TO_TIMEOUT = 0.3
LUMIGO_EVENT_KEY = "_lumigo"
STEP_FUNCTION_UID_KEY = "step_function_uid"
MAX_SIZE_FOR_REQUEST: int = int(os.environ.get("LUMIGO_MAX_SIZE_FOR_REQUEST", 900_000))
MAX_VARS_SIZE = 100000
MAX_VAR_LEN = 200
MAX_ENTRY_SIZE = 1024
FrameVariables = Dict[str, str]

_logger: Union[logging.Logger, None] = None


class Configuration:
    should_report: bool = True
    host: str = ""
    token: Optional[str] = ""
    verbose: bool = True
    enhanced_print: bool = False
    is_step_function: bool = False
    timeout_timer: bool = True
    send_only_if_error: bool = False


def config(
    edge_host: str = "",
    should_report: Union[bool, None] = None,
    token: Optional[str] = None,
    verbose: bool = True,
    enhance_print: bool = False,
    step_function: bool = False,
    timeout_timer: bool = True,
) -> None:
    """
    This function configure the lumigo wrapper.

    :param verbose: Whether the tracer should send all the possible information (debug mode)
    :param edge_host: The host to send the events. Leave empty for default.
    :param should_report: Weather we should send the events. Change to True in the production.
    :param token: The token to use when sending back the events.
    :param enhance_print: Should we add prefix to the print (so the logs will be in the platform).
    :param step_function: Is this function is a part of a step function?
    :param timeout_timer: Should we start a timer to send the traced data before timeout acceded.
    """
    if should_report is not None:
        Configuration.should_report = should_report
    elif not is_aws_environment():
        Configuration.should_report = False
    Configuration.host = edge_host or os.environ.get("LUMIGO_TRACER_HOST", "")
    Configuration.token = token or os.environ.get("LUMIGO_TRACER_TOKEN", "")
    Configuration.enhanced_print = enhance_print
    Configuration.verbose = verbose and os.environ.get("LUMIGO_VERBOSE", "").lower() != "false"
    Configuration.is_step_function = step_function
    Configuration.timeout_timer = timeout_timer
    Configuration.send_only_if_error = os.environ.get("SEND_ONLY_IF_ERROR", "").lower() == "true"


def _is_span_has_error(span: dict) -> bool:
    return (
        span.get("error") is not None  # noqa
        or span.get("info", {}).get("httpInfo", {}).get("response", {}).get("statusCode", 0)  # noqa
        > 400  # noqa
        or span.get("returnValue", {}).get("statusCode", 0) > 400  # noqa
    )


def _get_event_base64_size(event) -> int:
    return len(b64encode(json.dumps(event).encode()))


def _create_request_body(
    msgs: List[dict], prune_size_flag: bool, max_size: int = MAX_SIZE_FOR_REQUEST
) -> str:
    if not prune_size_flag or _get_event_base64_size(msgs) < max_size:
        return json.dumps(msgs)

    end_span = msgs[-1]
    ordered_spans = sorted(msgs[:-1], key=_is_span_has_error, reverse=True)

    spans_to_send: list = [end_span]
    for span in ordered_spans:
        current_size = _get_event_base64_size(spans_to_send)
        span_size = _get_event_base64_size(span)
        if current_size + span_size < max_size:
            spans_to_send.append(span)
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
        msg["token"] = Configuration.token
    get_logger().info(f"reporting the messages: {msgs}")
    host = Configuration.host or EDGE_HOST.format(region=region)
    duration = 0
    if Configuration.should_report:
        try:
            prune_trace: bool = not os.environ.get("LUMIGO_PRUNE_TRACE_OFF", "").lower() == "true"
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


def format_frames(frames_infos: List[inspect.FrameInfo]) -> List[dict]:
    free_space = MAX_VARS_SIZE
    frames: List[dict] = []
    for frame_info in reversed(frames_infos):
        if free_space <= 0 or "lumigo_tracer" in frame_info.filename:
            return frames
        frames.append(format_frame(frame_info, free_space))
        free_space -= len(json.dumps(frames[-1]))
    return frames


def format_frame(frame_info: inspect.FrameInfo, free_space: int) -> dict:
    return {
        "lineno": frame_info.lineno,
        "fileName": frame_info.filename,
        "function": frame_info.function,
        "variables": _truncate_locals(frame_info.frame.f_locals, free_space),
    }


def _truncate_locals(f_locals: Dict[str, Any], free_space: int) -> FrameVariables:
    """
    Truncate variable part or the entire variable in order to avoid exceeding the maximum frames size.
    :param f_locals: inspect.FrameInfo.frame.f_locals
    """
    locals_truncated: FrameVariables = {}
    for var_name, var_value in f_locals.items():
        var = {var_name: prepare_large_data(var_value, MAX_VAR_LEN)}
        free_space -= len(json.dumps(var))
        if free_space <= 0:
            return locals_truncated
        locals_truncated.update(var)
    return locals_truncated


def prepare_large_data(value: Union[str, bytes, dict, None], max_size=MAX_ENTRY_SIZE) -> str:
    """
    This function prepare the given value to send it to lumigo.
    You should call to this function if there's a possibility that the value will be big.

    Current logic:
        Converts the data to str and if it is larger than `max_size`, we truncate it.

    :param value: The value we wish to send
    :param max_size: The maximum size of the data that we will send
    :return: The value that we will actually send
    """
    if isinstance(value, dict):
        try:
            value = json.dumps(value)
        except Exception:
            pass
    elif isinstance(value, bytes):
        try:
            value = value.decode()
        except Exception:
            pass

    res = str(value)
    if len(res) > max_size:
        return f"{res[:max_size]}...[too long]"
    return res
