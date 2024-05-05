import copy
import datetime
import http.client
import os
import random
import socket
import time
import uuid
from base64 import b64encode
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from lumigo_core.configuration import CoreConfiguration

from lumigo_tracer.lumigo_utils import (
    EDGE_HOST,
    Configuration,
    InternalState,
    aws_dump,
    get_logger,
    get_region,
    internal_analytics_message,
    is_span_has_error,
    lumigo_safe_execute,
    should_use_tracer_extension,
    warn_client,
)

try:
    import boto3
    import botocore
except Exception:
    botocore = None
    boto3 = None

EDGE_PATH = "/api/spans"
HTTPS_PREFIX = "https://"
SECONDS_TO_TIMEOUT = 0.5
EDGE_TIMEOUT = float(os.environ.get("LUMIGO_EDGE_TIMEOUT", SECONDS_TO_TIMEOUT))
REQUEST_MAX_SIZE = 1024 * 990
MAX_SIZE_FOR_REQUEST: int = min(
    int(os.environ.get("LUMIGO_MAX_SIZE_FOR_REQUEST", 1024 * 500)), REQUEST_MAX_SIZE
)
MAX_SIZE_FOR_REQUEST_ON_ERROR: int = min(
    int(os.environ.get("LUMIGO_MAX_SIZE_FOR_REQUEST_ON_ERROR", 1024 * 990)), REQUEST_MAX_SIZE
)
MAX_NUMBER_OF_SPANS: int = int(os.environ.get("LUMIGO_MAX_NUMBER_OF_SPANS", 2000))
TOO_BIG_SPANS_THRESHOLD = 5
NUMBER_OF_SPANS_IN_REPORT_OPTIMIZATION = 200
COOLDOWN_AFTER_TIMEOUT_DURATION = datetime.timedelta(seconds=10)
CHINA_REGION = "cn-northwest-1"
LUMIGO_SPANS_DIR = "/tmp/lumigo-spans"
FUNCTION_TYPE = "function"
ENRICHMENT_TYPE = "enrichment"
HTTP_TYPE = "http"
MONGO_SPAN = "mongoDb"
REDIS_SPAN = "redis"
SQL_SPAN = "mySql"


edge_kinesis_boto_client = None
edge_connection = None


def establish_connection_global() -> None:
    global edge_connection
    try:
        # Try to establish the connection in initialization
        if (
            os.environ.get("LUMIGO_INITIALIZATION_CONNECTION", "").lower() != "false"
            and get_region() != CHINA_REGION  # noqa
        ):
            edge_connection = establish_connection()
            if edge_connection:
                edge_connection.connect()
    except socket.timeout:
        InternalState.mark_timeout_to_edge()
    except Exception:
        pass


def should_report_to_edge() -> bool:
    if not InternalState.timeout_on_connection:
        return True
    time_diff = datetime.datetime.now() - InternalState.timeout_on_connection
    return time_diff > COOLDOWN_AFTER_TIMEOUT_DURATION


def establish_connection(host: Optional[str] = None) -> Optional[http.client.HTTPSConnection]:
    try:
        if not host:
            host = get_edge_host(os.environ.get("AWS_REGION"))
        return http.client.HTTPSConnection(host, timeout=EDGE_TIMEOUT)
    except Exception as e:
        get_logger().exception(f"Could not establish connection to {host}", exc_info=e)
    return None


@lru_cache(maxsize=1)
def get_edge_host(region: Optional[str] = None) -> str:
    host = Configuration.host or EDGE_HOST.format(region=region or get_region())
    if host.startswith(HTTPS_PREFIX):
        host = host[len(HTTPS_PREFIX) :]  # noqa: E203
    if host.endswith(EDGE_PATH):
        host = host[: -len(EDGE_PATH)]
    return host


def report_json(
    region: Optional[str],
    msgs: List[Dict[Any, Any]],
    should_retry: bool = True,
    is_start_span: bool = False,
) -> int:
    """
    This function sends the information back to the edge.

    :param region: The region to use as default if not configured otherwise.
    :param msgs: the message to send.
    :param should_retry: False to disable the default retry on unsuccessful sending
    :param is_start_span: a flag to indicate if this is the start_span
     of spans that will be written
    :return: The duration of reporting (in milliseconds),
                or 0 if we didn't send (due to configuration or fail).
    """
    if not should_report_to_edge():
        get_logger().info("Skip sending messages due to previous timeout")
        return 0
    if not CoreConfiguration.should_report:
        return 0
    get_logger().info(f"reporting the messages: {msgs[:10]}")
    try:
        prune_trace: bool = not os.environ.get("LUMIGO_PRUNE_TRACE_OFF", "").lower() == "true"
        to_send = _create_request_body(msgs, prune_trace).encode()
    except Exception as e:
        get_logger().exception("Failed to create request: A span was lost.", exc_info=e)
        return 0
    if should_use_tracer_extension():
        with lumigo_safe_execute("report json file: writing spans to file"):
            write_spans_to_files(spans=msgs, is_start_span=is_start_span)
        return 0
    if region == CHINA_REGION:
        return _publish_spans_to_kinesis(to_send, CHINA_REGION)
    host = None
    global edge_connection
    with lumigo_safe_execute("report json: establish connection"):
        host = get_edge_host(region)
        duration = 0
        if not edge_connection or edge_connection.host != host:
            edge_connection = establish_connection(host)
            if not edge_connection:
                get_logger().warning("Can not establish connection. Skip sending span.")
                return duration
    try:
        start_time = time.time()
        edge_connection.request(
            "POST",
            EDGE_PATH,
            to_send,
            headers={
                "Content-Type": "application/json",
                "Authorization": Configuration.token or "",
            },
        )
        response = edge_connection.getresponse()
        response.read()  # We must read the response to keep the connection available
        duration = int((time.time() - start_time) * 1000)
        get_logger().info(f"successful reporting, code: {getattr(response, 'code', 'unknown')}")
    except socket.timeout:
        get_logger().exception(f"Timeout while connecting to {host}")
        InternalState.mark_timeout_to_edge()
        internal_analytics_message("report: socket.timeout")
    except Exception as e:
        if should_retry:
            get_logger().info(f"Could not report to {host}: ({str(e)}). Retrying.")
            edge_connection = establish_connection(host)
            report_json(region, msgs, should_retry=False)
        else:
            get_logger().exception("Could not report: A span was lost.", exc_info=e)
            internal_analytics_message(f"report: {type(e)}")
    return duration


def get_span_priority(span: Dict[Any, Any]) -> int:
    if span.get("type") == FUNCTION_TYPE:
        return 0
    if span.get("type") == ENRICHMENT_TYPE:
        return 1
    if is_span_has_error(span):
        return 2
    return 3


def get_span_metadata(span: Dict[Any, Any]) -> Dict[Any, Any]:
    with lumigo_safe_execute("get_span_metadata"):
        span_type = span.get("type")
        span_copy = copy.deepcopy(span)
        span_copy["isMetadata"] = True

        if span_type == FUNCTION_TYPE:
            span_copy.pop("envs", None)
            return span_copy
        if span_type == ENRICHMENT_TYPE:
            return {}
        if span_type == HTTP_TYPE:
            span_copy.get("info", {}).get("httpInfo", {}).get("request", {}).pop("headers", None)
            span_copy.get("info", {}).get("httpInfo", {}).get("request", {}).pop("body", None)
            span_copy.get("info", {}).get("httpInfo", {}).get("response", {}).pop("headers", None)
            span_copy.get("info", {}).get("httpInfo", {}).get("response", {}).pop("body", None)
            return span_copy
        if span_type == MONGO_SPAN:
            span_copy.pop("request", None)
            span_copy.pop("response", None)
            return span_copy
        if span_type == REDIS_SPAN:
            span_copy.pop("requestArgs", None)
            span_copy.pop("response", None)
            return span_copy
        if span_type == SQL_SPAN:
            span_copy.pop("query", None)
            span_copy.pop("values", None)
            span_copy.pop("response", None)
            return span_copy

    get_logger().warning(f"Got unsupported span type: {span_type}", extra={"span_type": span_type})
    return {}


def _get_prioritized_spans(
    msgs: List[dict], request_max_size: int, too_big_spans_threshold: int  # type: ignore[type-arg]
) -> List[dict]:  # type: ignore[type-arg]
    """
    When we exceed the request size limit, we need to apply the smart span selection.

    The smart span selection has 3 parts:
    1. We order the spans by the spans priority logic see get_span_priority.
    2. We take all the spans metadata,
    3 We take the full spans.
    We do steps 2 and 3 until we reach the request_max_size.
    """
    with lumigo_safe_execute("create_request_body: smart span selection"):
        get_logger().info("Starting smart span selection")
        # If we didn't send all the spans, we need to apply the smart span selection.
        ordered_spans = sorted(msgs, key=get_span_priority)
        current_size = 0
        too_big_spans = 0
        spans_to_send_sizes = {}
        spans_to_send_dict = {}

        # Take only spans metadata
        for index, span in enumerate(ordered_spans):
            spans_to_send_sizes[index] = 0
            span_metadata = get_span_metadata(span)
            if span_metadata == {}:
                continue
            span_metadata_size = get_event_base64_size(span_metadata)

            if (
                current_size + span_metadata_size < request_max_size
                or span.get("type") == FUNCTION_TYPE
            ):
                # We always want to at least send the function span
                spans_to_send_dict[index] = span_metadata
                spans_to_send_sizes[index] = span_metadata_size
                current_size += span_metadata_size
            else:
                # This is an optimization step. If the spans are too big, don't try to send them.
                too_big_spans += 1
                if too_big_spans >= too_big_spans_threshold:
                    break

        # Override basic spans with full spans
        for index, span in enumerate(ordered_spans):
            span_metadata_size = spans_to_send_sizes[index]
            span_size = get_event_base64_size(span)

            if current_size + span_size - span_metadata_size < request_max_size:
                spans_to_send_dict[index] = span
                current_size += span_size - span_metadata_size
            else:
                # This is an optimization step. If the spans are too big, don't try to send them.
                too_big_spans += 1
                if too_big_spans >= too_big_spans_threshold:
                    break
    return list(spans_to_send_dict.values())


def _create_request_body(
    msgs: List[dict],  # type: ignore[type-arg]
    prune_size_flag: bool,
    max_size: int = MAX_SIZE_FOR_REQUEST,
    max_error_size: int = MAX_SIZE_FOR_REQUEST_ON_ERROR,
    too_big_spans_threshold: int = TOO_BIG_SPANS_THRESHOLD,
) -> str:
    """
    This function creates the request body from the given spans.
    If there is an error we limit the size of the request to max_error_size otherwise to max_size.

    First we try to take all the spans and then we apply the smart span selection.

    The smart span selection has 2 parts:
    1. We order the spans by FUNCTION_SPAN, ERROR_HTTP_SPAN, ENRICHMENT_SPAN, HTTP_SPAN.
    2. We take all the spans metadata, We take the full spans. We do this until reach the max_size.
    """
    request_size_limit = max_error_size if any(map(is_span_has_error, msgs)) else max_size

    if not prune_size_flag or (
        len(msgs) < NUMBER_OF_SPANS_IN_REPORT_OPTIMIZATION
        and get_event_base64_size(msgs) < request_size_limit  # noqa
    ):
        return aws_dump(msgs)[:request_size_limit]

    current_size = 0
    spans_to_send: List[dict] = []  # type: ignore[type-arg]
    for index, span in enumerate(msgs):
        span_size = get_event_base64_size(span)
        if current_size + span_size > request_size_limit:
            break

        spans_to_send.append(span)
        current_size += span_size

    if len(spans_to_send) < len(msgs):
        selected_spans = _get_prioritized_spans(msgs, request_size_limit, too_big_spans_threshold)
        spans_to_send = sorted(selected_spans, key=get_span_priority)

    return aws_dump(spans_to_send)[:request_size_limit]


def write_spans_to_files(
    spans: List[Dict[Any, Any]], max_spans: int = MAX_NUMBER_OF_SPANS, is_start_span: bool = True
) -> None:
    to_send = spans[:max_spans]
    if is_start_span:
        get_logger().info("Creating start span file")
        write_extension_file(to_send, "span")
    else:
        get_logger().info("Creating end span file")
        write_extension_file(to_send, "end")


def write_extension_file(data: List[Dict], span_type: str):  # type: ignore[no-untyped-def,type-arg]
    Path(get_extension_dir()).mkdir(parents=True, exist_ok=True)
    to_send = aws_dump(data).encode()
    file_path = get_span_file_name(span_type)
    with open(file_path, "wb") as span_file:
        span_file.write(to_send)
        get_logger().info(f"Wrote span to file to [{file_path}][{len(to_send)}]")


def get_extension_dir() -> str:
    return (os.environ.get("LUMIGO_EXTENSION_SPANS_DIR_KEY") or LUMIGO_SPANS_DIR).lower()


def get_span_file_name(span_type: str):  # type: ignore[no-untyped-def]
    unique_name = str(uuid.uuid4())
    return os.path.join(get_extension_dir(), f"{unique_name}_{span_type}")


def _publish_spans_to_kinesis(to_send: bytes, region: str) -> int:
    start_time = time.time()
    try:
        get_logger().info("Sending spans to Kinesis")
        if not Configuration.edge_kinesis_aws_access_key_id:
            get_logger().error("Missing edge_kinesis_aws_access_key_id, can't publish the spans")
            return 0
        if not Configuration.edge_kinesis_aws_secret_access_key:
            get_logger().error(
                "Missing edge_kinesis_aws_secret_access_key, can't publish the spans"
            )
            return 0
        _send_data_to_kinesis(
            stream_name=Configuration.edge_kinesis_stream_name,
            to_send=to_send,
            region=region,
            aws_access_key_id=Configuration.edge_kinesis_aws_access_key_id,
            aws_secret_access_key=Configuration.edge_kinesis_aws_secret_access_key,
        )
    except Exception as err:
        get_logger().exception("Failed to send spans to Kinesis", exc_info=err)
        warn_client(f"Failed to send spans to Kinesis: {err}")
    return int((time.time() - start_time) * 1000)


def _send_data_to_kinesis(  # type: ignore[no-untyped-def]
    stream_name: str,
    to_send: bytes,
    region: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
):
    if not boto3:
        get_logger().error("boto3 is missing. Unable to send to Kinesis.")
        return None
    client = _get_edge_kinesis_boto_client(
        region=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    client.put_record(Data=to_send, StreamName=stream_name, PartitionKey=str(random.random()))
    get_logger().info("Successful sending to Kinesis")


def _get_edge_kinesis_boto_client(region: str, aws_access_key_id: str, aws_secret_access_key: str):  # type: ignore[no-untyped-def]
    global edge_kinesis_boto_client
    if not edge_kinesis_boto_client or _is_edge_kinesis_connection_cache_disabled():
        edge_kinesis_boto_client = boto3.client(
            "kinesis",
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=botocore.config.Config(retries={"max_attempts": 1, "mode": "standard"}),
        )
    return edge_kinesis_boto_client


def _is_edge_kinesis_connection_cache_disabled() -> bool:
    return os.environ.get("LUMIGO_KINESIS_SHOULD_REUSE_CONNECTION", "").lower() == "false"


def get_event_base64_size(event: Union[Dict[Any, Any], List[Dict[Any, Any]]]) -> int:
    return len(b64encode(aws_dump(event).encode()))
