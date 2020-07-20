import decimal
import hashlib
import json
import logging
import os
import re
import time
import urllib.request
from urllib.error import URLError
from collections import OrderedDict
from typing import Union, List, Optional, Dict, Any
from contextlib import contextmanager
from base64 import b64encode
import inspect

from lumigo_tracer.libs.custom_json_dump import CustomObjectEncoder

EXECUTION_TAGS_KEY = "lumigo_execution_tags_no_scrub"
EDGE_HOST = "https://{region}.lumigo-tracer-edge.golumigo.com/api/spans"
LOG_FORMAT = "#LUMIGO# - %(asctime)s - %(levelname)s - %(message)s"
SECONDS_TO_TIMEOUT = 0.5
LUMIGO_EVENT_KEY = "_lumigo"
STEP_FUNCTION_UID_KEY = "step_function_uid"
# number of spans that are too big to enter the reported message before break
TOO_BIG_SPANS_THRESHOLD = 5
MAX_SIZE_FOR_REQUEST: int = int(os.environ.get("LUMIGO_MAX_SIZE_FOR_REQUEST", 900_000))
MAX_VARS_SIZE = 100_000
MAX_VAR_LEN = 200
DEFAULT_MAX_ENTRY_SIZE = 2048
FrameVariables = Dict[str, str]
OMITTING_KEYS_REGEXES = [
    ".*pass.*",
    ".*key.*",
    ".*secret.*",
    ".*credential.*",
    "SessionToken",
    "x-amz-security-token",
    "Signature",
    "Authorization",
]
DOMAIN_SCRUBBER_REGEXES = [
    r"secretsmanager\..*\.amazonaws\.com",
    r"ssm\..*\.amazonaws\.com",
    r"kms\..*\.amazonaws\.com",
    r"sts\..*amazonaws\.com",
]
SKIP_SCRUBBING_KEYS = [EXECUTION_TAGS_KEY]
LUMIGO_SECRET_MASKING_REGEX_BACKWARD_COMP = "LUMIGO_BLACKLIST_REGEX"
LUMIGO_SECRET_MASKING_REGEX = "LUMIGO_SECRET_MASKING_REGEX"
WARN_CLIENT_PREFIX = "Lumigo Warning"
TRUNCATE_SUFFIX = "...[too long]"
NUMBER_OF_SPANS_IN_REPORT_OPTIMIZATION = 200

_logger: Union[logging.Logger, None] = None


class Configuration:
    should_report: bool = True
    host: str = ""
    token: Optional[str] = ""
    verbose: bool = True
    enhanced_print: bool = False
    is_step_function: bool = False
    timeout_timer: bool = True
    timeout_timer_buffer: Optional[float] = None
    send_only_if_error: bool = False
    domains_scrubber: Optional[List] = None
    max_entry_size: int = DEFAULT_MAX_ENTRY_SIZE


def config(
    edge_host: str = "",
    should_report: Union[bool, None] = None,
    token: Optional[str] = None,
    verbose: bool = True,
    enhance_print: bool = False,
    step_function: bool = False,
    timeout_timer: bool = True,
    timeout_timer_buffer: Optional[float] = None,
    domains_scrubber: Optional[List[str]] = None,
    max_entry_size: int = DEFAULT_MAX_ENTRY_SIZE,
) -> None:
    """
    This function configure the lumigo wrapper.

    :param verbose: Whether the tracer should send all the possible information (debug mode)
    :param edge_host: The host to send the events. Leave empty for default.
    :param should_report: Whether we should send the events. Change to True in the production.
    :param token: The token to use when sending back the events.
    :param enhance_print: Should we add prefix to the print (so the logs will be in the platform).
    :param step_function: Is this function is a part of a step function?
    :param timeout_timer: Should we start a timer to send the traced data before timeout acceded.
    :param timeout_timer_buffer: The buffer (seconds) that we take before reaching timeout to send the traces to lumigo.
        The default is 10% of the duration of the lambda (with upper and lower bounds of 0.5 and 3 seconds).
    :param domains_scrubber: List of regexes. We will not collect data of requests with hosts that match it.
    :param max_entry_size: The maximum size of each entry when sending back the events.
    """
    if should_report is not None:
        Configuration.should_report = should_report
    elif not is_aws_environment():
        Configuration.should_report = False
    Configuration.host = edge_host or os.environ.get("LUMIGO_TRACER_HOST", "")
    Configuration.token = token or os.environ.get("LUMIGO_TRACER_TOKEN", "")
    Configuration.enhanced_print = (
        enhance_print or os.environ.get("LUMIGO_ENHANCED_PRINT", "").lower() == "true"
    )
    Configuration.verbose = verbose and os.environ.get("LUMIGO_VERBOSE", "").lower() != "false"
    Configuration.is_step_function = (
        step_function or os.environ.get("LUMIGO_STEP_FUNCTION", "").lower() == "true"
    )
    Configuration.timeout_timer = timeout_timer
    try:
        if "LUMIGO_TIMEOUT_BUFFER" in os.environ:
            Configuration.timeout_timer_buffer = float(os.environ["LUMIGO_TIMEOUT_BUFFER"])
        else:
            Configuration.timeout_timer_buffer = timeout_timer_buffer
    except Exception:
        warn_client("Could not configure LUMIGO_TIMEOUT_BUFFER. Using default value.")
        Configuration.timeout_timer_buffer = None
    Configuration.send_only_if_error = os.environ.get("SEND_ONLY_IF_ERROR", "").lower() == "true"
    if domains_scrubber:
        domains_scrubber_regex = domains_scrubber
    elif "LUMIGO_DOMAINS_SCRUBBER" in os.environ:
        try:
            domains_scrubber_regex = json.loads(os.environ["LUMIGO_DOMAIN_SCRUBBER"])
        except Exception:
            get_logger().critical(
                "Could not parse the specified domains scrubber, shutting down the reporter."
            )
            Configuration.should_report = False
            domains_scrubber_regex = []
    else:
        domains_scrubber_regex = DOMAIN_SCRUBBER_REGEXES
    Configuration.domains_scrubber = [re.compile(r, re.IGNORECASE) for r in domains_scrubber_regex]
    Configuration.max_entry_size = int(os.environ.get("LUMIGO_MAX_ENTRY_SIZE", max_entry_size))


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
    msgs: List[dict],
    prune_size_flag: bool,
    max_size: int = MAX_SIZE_FOR_REQUEST,
    too_big_spans_threshold: int = TOO_BIG_SPANS_THRESHOLD,
) -> str:

    if not prune_size_flag or (
        len(msgs) < NUMBER_OF_SPANS_IN_REPORT_OPTIMIZATION
        and _get_event_base64_size(msgs) < max_size  # noqa
    ):
        return lumigo_dumps(msgs, max_size=max_size)

    end_span = msgs[-1]
    ordered_spans = sorted(msgs[:-1], key=_is_span_has_error, reverse=True)

    spans_to_send: list = [end_span]
    current_size = _get_event_base64_size(end_span)
    too_big_spans = 0
    for span in ordered_spans:
        span_size = _get_event_base64_size(span)
        if current_size + span_size < max_size:
            spans_to_send.append(span)
            current_size += span_size
        else:
            # This is an optimization step. If the spans are too big, don't try to send them.
            too_big_spans += 1
            if too_big_spans == too_big_spans_threshold:
                break
    return lumigo_dumps(spans_to_send, max_size=max_size)


def report_json(region: Union[None, str], msgs: List[dict]) -> int:
    """
    This function sends the information back to the edge.

    :param region: The region to use as default if not configured otherwise.
    :param msgs: the message to send.
    :return: The duration of reporting (in milliseconds),
                or 0 if we didn't send (due to configuration or fail).
    """
    get_logger().info(f"reporting the messages: {msgs[:10]}")
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


def ensure_str(s: Union[str, bytes]) -> str:
    return s if isinstance(s, str) else s.decode()


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
        "variables": _truncate_locals(
            frame_info.frame.f_locals, free_space, get_omitting_regexes()
        ),
    }


def _truncate_locals(
    f_locals: Dict[str, Any], free_space: int, omitting_regexes: list
) -> FrameVariables:
    """
    Truncate variable part or the entire variable in order to avoid exceeding the maximum frames size.
    :param f_locals: inspect.FrameInfo.frame.f_locals
    """
    locals_truncated: FrameVariables = {}
    for var_name, var_value in f_locals.items():
        if any(r.match(var_name) for r in omitting_regexes):  # type: ignore
            var = {var_name: "****"}
        else:
            var = {var_name: lumigo_dumps(var_value, max_size=MAX_VAR_LEN)}
        free_space -= len(json.dumps(var))
        if free_space <= 0:
            return locals_truncated
        locals_truncated.update(var)
    return locals_truncated


def get_omitting_regexes():
    if LUMIGO_SECRET_MASKING_REGEX in os.environ:
        given_regexes = json.loads(os.environ[LUMIGO_SECRET_MASKING_REGEX])
    elif LUMIGO_SECRET_MASKING_REGEX_BACKWARD_COMP in os.environ:
        given_regexes = json.loads(os.environ[LUMIGO_SECRET_MASKING_REGEX_BACKWARD_COMP])
    else:
        given_regexes = OMITTING_KEYS_REGEXES
    return [re.compile(r, re.IGNORECASE) for r in given_regexes]


def warn_client(msg: str) -> None:
    if os.environ.get("LUMIGO_WARNINGS") != "off":
        print(f"{WARN_CLIENT_PREFIX}: {msg}")


def is_api_gw_event(event: dict) -> bool:
    return bool(
        isinstance(event, Dict)
        and event.get("requestContext")  # noqa
        and event.get("requestContext", {}).get("domainName")  # noqa
        and event.get("requestContext", {}).get("requestId")  # noqa
    )


def get_timeout_buffer(remaining_time: float):
    buffer = Configuration.timeout_timer_buffer
    if not buffer:
        buffer = max(0.5, min(0.1 * remaining_time, 3))
    return buffer


def md5hash(d: dict) -> str:
    h = hashlib.md5()
    h.update(json.dumps(d, sort_keys=True).encode())
    return h.hexdigest()


class LumigoEncoder(CustomObjectEncoder):
    class OmittedDict(dict):
        pass

    class OmittedStr(str):
        pass

    def __init__(self, regexes, enforce_jsonify):
        self.regexes = regexes
        self.enforce_jsonify = enforce_jsonify
        super().__init__()

    def default(self, o):
        if isinstance(o, decimal.Decimal):
            # copied from python's runtime: runtime/lambda_runtime_marshaller.py:7-11
            return float(o)
        if isinstance(o, bytes):
            try:
                return o.decode()
            except Exception:
                return str(o)
        if isinstance(o, dict):
            if not self.regexes:
                return self.OmittedDict(o)
            items = {}
            for k, v in o.items():
                if k in SKIP_SCRUBBING_KEYS:
                    items[k] = self.OmittedDict(v)
                elif isinstance(k, str) and any(r.match(k) for r in self.regexes):  # type: ignore
                    items[k] = "****"
                else:
                    items[k] = v
            return self.OmittedDict(items)
        if isinstance(o, str):
            try:
                parsed_value = json.loads(o)
                if isinstance(parsed_value, dict):
                    return parsed_value
            except Exception:
                return self.OmittedStr(o)
        try:
            if not self.enforce_jsonify:
                return str(o)
        except Exception:
            pass
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

    def should_use_original(self, o, cls):
        if isinstance(o, (OrderedDict, dict, decimal.Decimal)) and not isinstance(
            o, self.OmittedDict
        ):
            return False
        if isinstance(o, bytes):
            return False
        if isinstance(o, str) and not isinstance(o, self.OmittedStr):
            # This is an optimization step. Fast identify if this is a dict
            if o.startswith("{"):
                return False
        return isinstance(o, cls)


def lumigo_dumps(d: Any, max_size, regexes: List = None, enforce_jsonify: bool = False):
    regexes = regexes if regexes is not None else get_omitting_regexes()

    retval = ""
    try:
        for chunk in LumigoEncoder(regexes, enforce_jsonify).iterencode(d):
            retval += chunk
            if len(retval) > max_size:
                return LumigoEncoder.OmittedStr(retval[:max_size] + TRUNCATE_SUFFIX)
    except Exception:
        if enforce_jsonify:
            raise
    return LumigoEncoder.OmittedStr(retval)
