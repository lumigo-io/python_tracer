import base64
import datetime
import inspect
import logging
import os
import re
import traceback
import uuid
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Pattern, TypeVar, Union

from lumigo_core.configuration import (
    MASKING_REGEX_ENVIRONMENT,
    MASKING_REGEX_HTTP_QUERY_PARAMS,
    MASKING_REGEX_HTTP_REQUEST_BODIES,
    MASKING_REGEX_HTTP_REQUEST_HEADERS,
    MASKING_REGEX_HTTP_RESPONSE_BODIES,
    MASKING_REGEX_HTTP_RESPONSE_HEADERS,
    CoreConfiguration,
    create_regex_from_list,
    parse_regex_from_env,
)
from lumigo_core.logger import get_logger
from lumigo_core.lumigo_utils import aws_dump, get_current_ms_time
from lumigo_core.scrubbing import (
    TRUNCATE_SUFFIX,
    lumigo_dumps,
    lumigo_dumps_with_context,
    omit_keys,
)

LUMIGO_DOMAINS_SCRUBBER_KEY = "LUMIGO_DOMAINS_SCRUBBER"
EDGE_SUFFIX = "golumigo.com"
EDGE_HOST = "{region}.lumigo-tracer-edge." + EDGE_SUFFIX
LUMIGO_EVENT_KEY = "_lumigo"
STEP_FUNCTION_UID_KEY = "step_function_uid"
# number of spans that are too big to enter the reported message before break
MAX_VARS_SIZE = 100_000
MAX_VAR_LEN = 1024
FrameVariables = Dict[str, str]

DOMAIN_SCRUBBER_REGEXES = [
    r"secretsmanager\..*\.amazonaws\.com",
    r"ssm\..*\.amazonaws\.com",
    r"kms\..*\.amazonaws\.com",
    r"sts\..*amazonaws\.com",
]
LUMIGO_SYNC_TRACING = "LUMIGO_SYNC_TRACING"
LUMIGO_PROPAGATE_W3C = "LUMIGO_PROPAGATE_W3C"
WARN_CLIENT_PREFIX = "Lumigo Warning"
INTERNAL_ANALYTICS_PREFIX = "Lumigo Analytic Log"
DEFAULT_KEY_DEPTH = 4
LUMIGO_TOKEN_KEY = "LUMIGO_TRACER_TOKEN"
LUMIGO_USE_TRACER_EXTENSION = "LUMIGO_USE_TRACER_EXTENSION"
KILL_SWITCH = "LUMIGO_SWITCH_OFF"
EDGE_KINESIS_STREAM_NAME = "prod_trc-inges-edge_edge-kinesis-stream"
STACKTRACE_LINE_TO_DROP = "lumigo_tracer/lambda_tracer/tracer.py"
Container = TypeVar("Container", dict, list)  # type: ignore[type-arg,type-arg]
DEFAULT_AUTO_TAG_KEY = "LUMIGO_AUTO_TAG"
SKIP_COLLECTING_HTTP_BODY_KEY = "LUMIGO_SKIP_COLLECTING_HTTP_BODY"


def should_use_tracer_extension() -> bool:
    return (os.environ.get(LUMIGO_USE_TRACER_EXTENSION) or "false").lower() == "true"


def get_region() -> str:
    return os.environ.get("AWS_REGION") or "UNKNOWN"


class InternalState:
    timeout_on_connection: Optional[datetime.datetime] = None
    internal_error_already_logged = False

    @staticmethod
    def reset():  # type: ignore[no-untyped-def]
        InternalState.timeout_on_connection = None
        InternalState.internal_error_already_logged = False

    @staticmethod
    def mark_timeout_to_edge():  # type: ignore[no-untyped-def]
        InternalState.timeout_on_connection = datetime.datetime.now()


class Configuration:
    host: str = ""
    token: Optional[str] = ""
    verbose: bool = True
    is_step_function: bool = False
    timeout_timer: bool = True
    timeout_timer_buffer: Optional[float] = None
    send_only_if_error: bool = False
    domains_scrubber: Optional[Pattern[str]] = None
    get_key_depth: int = DEFAULT_KEY_DEPTH
    edge_kinesis_stream_name: str = EDGE_KINESIS_STREAM_NAME
    edge_kinesis_aws_access_key_id: Optional[str] = None
    edge_kinesis_aws_secret_access_key: Optional[str] = None
    is_sync_tracer: bool = False
    auto_tag: List[str] = []
    skip_collecting_http_body: bool = False
    propagate_w3c: bool = False
    secret_masking_regex_http_request_bodies: Optional[Pattern[str]] = None
    secret_masking_regex_http_request_headers: Optional[Pattern[str]] = None
    secret_masking_regex_http_response_bodies: Optional[Pattern[str]] = None
    secret_masking_regex_http_response_headers: Optional[Pattern[str]] = None
    secret_masking_regex_http_query_params: Optional[Pattern[str]] = None
    secret_masking_regex_environment: Optional[Pattern[str]] = None


def config(
    edge_host: str = "",
    should_report: Optional[bool] = None,
    token: Optional[str] = None,
    verbose: bool = True,
    enhance_print: bool = False,
    step_function: bool = False,
    timeout_timer: bool = True,
    timeout_timer_buffer: Optional[float] = None,
    domains_scrubber: Optional[List[str]] = None,
    max_entry_size: Optional[int] = None,
    get_key_depth: int = None,
    edge_kinesis_stream_name: Optional[str] = None,
    edge_kinesis_aws_access_key_id: Optional[str] = None,
    edge_kinesis_aws_secret_access_key: Optional[str] = None,
    auto_tag: Optional[List[str]] = None,
    skip_collecting_http_body: bool = False,
    propagate_w3c: bool = False,
) -> None:
    """
    This function configure the lumigo wrapper.

    :param verbose: Whether the tracer should send all the possible information (debug mode)
    :param edge_host: The host to send the events. Leave empty for default.
    :param should_report: Whether we should send the events. Change to True in the production.
    :param token: The token to use when sending back the events.
    :param enhance_print: Deprecated - Should we add prefix to the print (so the logs will be in the platform).
    :param step_function: Is this function is a part of a step function?
    :param timeout_timer: Should we start a timer to send the traced data before timeout acceded.
    :param timeout_timer_buffer: The buffer (seconds) that we take before reaching timeout to send the traces to lumigo.
        The default is 10% of the duration of the lambda (with upper and lower bounds of 0.5 and 3 seconds).
    :param domains_scrubber: List of regexes. We will not collect data of requests with hosts that match it.
    :param max_entry_size: The maximum size of each entry when sending back the events.
    :param get_key_depth: Max depth to search the lumigo key in the event (relevant to step functions). default 4.
    :param edge_kinesis_stream_name: The name of the Kinesis to push the spans in China region
    :param edge_kinesis_aws_access_key_id: The credentials to push to the Kinesis in China region
    :param edge_kinesis_aws_secret_access_key: The credentials to push to the Kinesis in China region
    :param auto_tag: The keys from the event that should be used as execution tags.
    :param skip_collecting_http_body: Should we not collect the HTTP request and response bodies.
    :param propagate_w3c: Should we add W3C headers to the lambda's HTTP requests.
    """

    Configuration.token = token or os.environ.get(LUMIGO_TOKEN_KEY, "")
    if not (Configuration.token and re.match("[t][_][a-z0-9]{15,100}", Configuration.token)):
        if token is not None:
            warn_client("Invalid Token. Go to Lumigo Settings to get a valid token.")
            should_report = False

    if should_report is not None:
        CoreConfiguration.should_report = should_report
    elif not is_aws_environment():
        CoreConfiguration.should_report = False
    Configuration.host = edge_host or os.environ.get("LUMIGO_TRACER_HOST", "")
    Configuration.verbose = verbose and os.environ.get("LUMIGO_VERBOSE", "").lower() != "false"
    Configuration.get_key_depth = get_key_depth or int(
        os.environ.get("LUMIGO_EVENT_KEY_DEPTH", DEFAULT_KEY_DEPTH)
    )
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
    Configuration.domains_scrubber = (
        create_regex_from_list(domains_scrubber)
        or parse_regex_from_env(LUMIGO_DOMAINS_SCRUBBER_KEY)
        or create_regex_from_list(DOMAIN_SCRUBBER_REGEXES)
    )
    if max_entry_size:
        CoreConfiguration.max_entry_size = max_entry_size
    Configuration.edge_kinesis_stream_name = (
        edge_kinesis_stream_name
        or os.environ.get("LUMIGO_EDGE_KINESIS_STREAM_NAME")  # noqa
        or EDGE_KINESIS_STREAM_NAME  # noqa
    )
    Configuration.edge_kinesis_aws_access_key_id = edge_kinesis_aws_access_key_id or os.environ.get(
        "LUMIGO_EDGE_KINESIS_AWS_ACCESS_KEY_ID"
    )
    Configuration.edge_kinesis_aws_secret_access_key = (
        edge_kinesis_aws_secret_access_key
        or os.environ.get("LUMIGO_EDGE_KINESIS_AWS_SECRET_ACCESS_KEY")  # noqa
    )
    Configuration.is_sync_tracer = os.environ.get(LUMIGO_SYNC_TRACING, "FALSE").lower() == "true"
    Configuration.propagate_w3c = (
        propagate_w3c or os.environ.get(LUMIGO_PROPAGATE_W3C, "true").lower() == "true"
    )
    Configuration.auto_tag = auto_tag or os.environ.get(
        "LUMIGO_AUTO_TAG", DEFAULT_AUTO_TAG_KEY
    ).split(",")
    Configuration.skip_collecting_http_body = (
        not Configuration.verbose
        or skip_collecting_http_body  # noqa: W503
        or os.environ.get(SKIP_COLLECTING_HTTP_BODY_KEY, "false").lower() == "true"  # noqa: W503
    )
    Configuration.secret_masking_regex_http_request_bodies = parse_regex_from_env(
        MASKING_REGEX_HTTP_REQUEST_BODIES
    )
    Configuration.secret_masking_regex_http_request_headers = parse_regex_from_env(
        MASKING_REGEX_HTTP_REQUEST_HEADERS
    )
    Configuration.secret_masking_regex_http_response_bodies = parse_regex_from_env(
        MASKING_REGEX_HTTP_RESPONSE_BODIES
    )
    Configuration.secret_masking_regex_http_response_headers = parse_regex_from_env(
        MASKING_REGEX_HTTP_RESPONSE_HEADERS
    )
    Configuration.secret_masking_regex_http_query_params = parse_regex_from_env(
        MASKING_REGEX_HTTP_QUERY_PARAMS
    )
    Configuration.secret_masking_regex_environment = parse_regex_from_env(MASKING_REGEX_ENVIRONMENT)


def is_span_has_error(span: dict) -> bool:  # type: ignore[type-arg]
    return (
        span.get("error") is not None  # noqa
        or span.get("info", {}).get("httpInfo", {}).get("response", {}).get("statusCode", 0)  # noqa
        > 400  # noqa
        or span.get("returnValue", {}).get("statusCode", 0) > 400  # noqa
    )


@contextmanager
def lumigo_safe_execute(part_name="", severity=logging.ERROR):  # type: ignore[no-untyped-def]
    try:
        yield
    except Exception as e:
        get_logger().log(
            severity, f"An exception occurred in lumigo's code {part_name}", exc_info=e
        )


def is_aws_environment() -> bool:
    """
    :return: heuristically determine rather we're running on an aws environment.
    """
    return bool(os.environ.get("AWS_LAMBDA_FUNCTION_VERSION"))


def ensure_str(s: Union[str, bytes]) -> str:
    return s if isinstance(s, str) else s.decode()


def format_frames(frames_infos: List[inspect.FrameInfo]) -> List[dict]:  # type: ignore[type-arg]
    free_space = MAX_VARS_SIZE
    frames: List[dict] = []  # type: ignore[type-arg]
    for frame_info in reversed(frames_infos):
        if free_space <= 0 or "lumigo_tracer" in frame_info.filename:
            return frames
        frames.append(format_frame(frame_info, free_space))
        free_space -= len(aws_dump(frames[-1], decimal_safe=True))
    return frames


def format_frame(frame_info: inspect.FrameInfo, free_space: int) -> dict:  # type: ignore[type-arg]
    return {
        "lineno": frame_info.lineno,
        "fileName": frame_info.filename,
        "function": frame_info.function,
        "variables": _truncate_locals(omit_keys(frame_info.frame.f_locals)[0], free_space),
    }


def _truncate_locals(f_locals: Dict[str, Any], free_space: int) -> FrameVariables:
    """
    Truncate variable part or the entire variable in order to avoid exceeding the maximum frames size.
    :param f_locals: inspect.FrameInfo.frame.f_locals
    """
    locals_truncated: FrameVariables = {}
    for var_name, var_value in f_locals.items():
        var = {var_name: lumigo_dumps(var_value, max_size=MAX_VAR_LEN, decimal_safe=True)}
        free_space -= len(aws_dump(var))
        if free_space <= 0:
            return locals_truncated
        locals_truncated.update(var)
    return locals_truncated


def warn_client(msg: str) -> None:
    if os.environ.get("LUMIGO_WARNINGS") != "off":
        print(f"{WARN_CLIENT_PREFIX}: {msg}")


def internal_analytics_message(msg: str, force: bool = False) -> None:
    if os.environ.get("LUMIGO_ANALYTICS") != "off":
        if force or not InternalState.internal_error_already_logged:
            b64_message = base64.b64encode(msg.encode()).decode()
            print(f"{INTERNAL_ANALYTICS_PREFIX}: {b64_message}")
            InternalState.internal_error_already_logged = True


def is_api_gw_event(event: dict) -> bool:  # type: ignore[type-arg]
    return bool(
        isinstance(event, Dict)
        and event.get("requestContext")  # noqa
        and event.get("requestContext", {}).get("domainName")  # noqa
        and event.get("requestContext", {}).get("requestId")  # noqa
    )


def create_step_function_span(message_id: str):  # type: ignore[no-untyped-def]
    return {
        "id": str(uuid.uuid4()),
        "type": "http",
        "info": {
            "resourceName": "StepFunction",
            "messageId": message_id,
            "httpInfo": {"host": "StepFunction", "request": {"method": "", "body": ""}},
        },
        "started": get_current_ms_time(),
    }


def get_timeout_buffer(remaining_time: float):  # type: ignore[no-untyped-def]
    buffer = Configuration.timeout_timer_buffer
    if not buffer:
        buffer = max(0.5, min(0.1 * remaining_time, 3))
    return buffer


def concat_old_body_to_new(context: str, old_body: Optional[str], new_body: bytes) -> str:
    """
    We have only a dumped body from the previous request,
    so to concatenate the new body we should undo the lumigo_dumps.
    Note that the old body is dumped bytes
    """
    if not new_body:
        return old_body or ""
    if not old_body:
        return lumigo_dumps_with_context(context, new_body)
    if old_body.endswith(TRUNCATE_SUFFIX):
        return old_body
    undumped_body = (old_body or "").encode().strip(b'"')
    return lumigo_dumps_with_context(context, undumped_body + new_body)


def is_kill_switch_on():  # type: ignore[no-untyped-def]
    return str(os.environ.get(KILL_SWITCH, "")).lower() == "true"


def get_size_upper_bound() -> int:
    return CoreConfiguration.get_max_entry_size(True)


def is_error_code(status_code: int) -> bool:
    return status_code >= 400


def is_aws_arn(string_to_validate: Optional[str]) -> bool:
    return bool(string_to_validate and string_to_validate.startswith("arn:aws:"))


def is_provision_concurrency_initialization() -> bool:
    return os.environ.get("AWS_LAMBDA_INITIALIZATION_TYPE") == "provisioned-concurrency"


def get_stacktrace(exception: Exception) -> str:
    original_traceback = traceback.format_tb(exception.__traceback__)
    return "".join(filter(lambda line: STACKTRACE_LINE_TO_DROP not in line, original_traceback))


def is_python_37() -> bool:
    return os.environ.get("AWS_EXECUTION_ENV") == "AWS_Lambda_python3.7"


def is_lambda_traced() -> bool:
    return (not is_kill_switch_on()) and is_aws_environment()
