import inspect
import os
import time
import uuid
import signal
import traceback
import http.client
from typing import List, Dict, Tuple, Optional, Callable, Set

from lumigo_tracer.parsers.event_parser import EventParser
from lumigo_tracer.utils import (
    Configuration,
    LUMIGO_EVENT_KEY,
    STEP_FUNCTION_UID_KEY,
    format_frames,
    prepare_large_data,
    omit_keys,
    EXECUTION_TAGS_KEY,
)
from lumigo_tracer import utils
from lumigo_tracer.parsers.parser import get_parser, HTTP_TYPE, StepFunctionParser
from lumigo_tracer.parsers.utils import (
    parse_trace_id,
    safe_split_get,
    recursive_json_join,
    parse_triggered_by,
)
from lumigo_tracer.utils import get_logger, _is_span_has_error
from .parsers.http_data_classes import HttpRequest

_VERSION_PATH = os.path.join(os.path.dirname(__file__), "VERSION")
MAX_LAMBDA_TIME = 15 * 60 * 1000
MAX_BODY_SIZE = 1024


class SpansContainer:
    is_cold = True
    _span: Optional["SpansContainer"] = None

    def __init__(
        self,
        name: str = None,
        started: int = None,
        region: str = None,
        runtime: str = None,
        memory_allocated: str = None,
        log_stream_name: str = None,
        log_group_name: str = None,
        trace_root: str = None,
        transaction_id: str = None,
        request_id: str = None,
        account: str = None,
        trace_id_suffix: str = None,
        trigger_by: dict = None,
        max_finish_time: int = None,
        event: str = None,
        envs: str = None,
    ):
        version = open(_VERSION_PATH, "r").read() if os.path.exists(_VERSION_PATH) else "unknown"
        version = version.strip()
        self.name = name
        self.region = region
        self.trace_root = trace_root
        self.trace_id_suffix = trace_id_suffix
        self.transaction_id = transaction_id
        self.max_finish_time = max_finish_time
        self.base_msg = {
            "started": started,
            "transactionId": transaction_id,
            "account": account,
            "region": region,
            "parentId": request_id,
            "info": {"tracer": {"version": version}, "traceId": {"Root": trace_root}},
            "event": event,
            "envs": envs,
        }
        self.function_span = recursive_json_join(
            {
                "id": request_id,
                "type": "function",
                "name": name,
                "runtime": runtime,
                "memoryAllocated": memory_allocated,
                "readiness": "cold" if SpansContainer.is_cold else "warm",
                "info": {
                    "logStreamName": log_stream_name,
                    "logGroupName": log_group_name,
                    **(trigger_by or {}),
                },
                EXECUTION_TAGS_KEY: [],
            },
            self.base_msg,
        )
        self.previous_request: Tuple[Optional[http.client.HTTPMessage], bytes] = (None, b"")
        self.previous_response_body: bytes = b""
        self.http_span_ids_to_send: Set[str] = set()
        self.http_spans: List[Dict] = []
        SpansContainer.is_cold = False

    def start(self, event=None, context=None):
        to_send = self.function_span.copy()
        to_send["id"] = f"{to_send['id']}_started"
        to_send["ended"] = to_send["started"]
        to_send["maxFinishTime"] = self.max_finish_time
        if not Configuration.send_only_if_error:
            report_duration = utils.report_json(region=self.region, msgs=[to_send])
            self.function_span["reporter_rtt"] = report_duration
            self.http_spans = []
        else:
            get_logger().debug("Skip sending start because tracer in 'send only if error' mode .")
        self.start_timeout_timer(context)

    def handle_timeout(self, *args):
        get_logger().info("The tracer reached the end of the timeout timer")
        to_send = [s for s in self.http_spans if s["id"] in self.http_span_ids_to_send]
        utils.report_json(region=self.region, msgs=to_send)
        self.http_span_ids_to_send.clear()

    def start_timeout_timer(self, context=None) -> None:
        if Configuration.timeout_timer and not Configuration.send_only_if_error:
            if not hasattr(context, "get_remaining_time_in_millis"):
                get_logger().info("Skip setting timeout timer - Could not get the remaining time.")
                return
            remaining_time = context.get_remaining_time_in_millis() / 1000
            if Configuration.timeout_timer_buffer >= remaining_time:
                get_logger().debug("Skip setting timeout timer - Too short timeout.")
                return
            TimeoutMechanism.start(
                remaining_time - Configuration.timeout_timer_buffer, self.handle_timeout
            )

    def add_request_event(self, parse_params: HttpRequest):
        """
            This function parses an request event and add it to the span.
        """
        parser = get_parser(parse_params.host)()
        msg = parser.parse_request(parse_params)
        self.previous_request = parse_params.headers, parse_params.body
        self.http_spans.append(recursive_json_join(msg, self.base_msg))
        self.http_span_ids_to_send.add(msg["id"])

    def add_unparsed_request(self, parse_params: HttpRequest):
        """
        This function handle the case where we got a request the is not fully formatted as we expected,
        I.e. there isn't '\r\n' in the request data that <i>logically</i> splits the headers from the body.

        In that case, we will consider it as a continuance of the previous request if they got the same url,
            and we didn't get any answer yet.
        """
        if self.http_spans:
            last_event = self.http_spans[-1]
            if last_event and last_event.get("type") == HTTP_TYPE:
                if last_event.get("info", {}).get("httpInfo", {}).get("host") == parse_params.host:
                    if "response" not in last_event["info"]["httpInfo"]:
                        self.http_spans.pop()
                        prev_headers, prev_body = self.previous_request
                        body = (prev_body + parse_params.body)[:MAX_BODY_SIZE]
                        self.add_request_event(parse_params.clone(headers=prev_headers, body=body))
                        return
        self.add_request_event(parse_params.clone(headers=None))

    def update_event_end_time(self) -> None:
        """
        This function assumes synchronous execution - we update the last http event.
        """
        if self.http_spans:
            self.http_spans[-1]["ended"] = int(time.time() * 1000)

    def update_event_response(
        self, host: Optional[str], status_code: int, headers: http.client.HTTPMessage, body: bytes
    ) -> None:
        """
        :param host: If None, use the host from the last span, otherwise this is the first chuck and we can empty
                            the aggregated response body
        This function assumes synchronous execution - we update the last http event.
        """
        if self.http_spans:
            last_event = self.http_spans.pop()
            if not host:
                host = last_event.get("info", {}).get("httpInfo", {}).get("host", "unknown")
            else:
                self.previous_response_body = b""

            parser = get_parser(host, headers)()  # type: ignore
            self.previous_response_body += body
            update = parser.parse_response(  # type: ignore
                host, status_code, headers, self.previous_response_body
            )
            self.http_spans.append(recursive_json_join(update, last_event))
            self.http_span_ids_to_send.add(update.get("id") or last_event["id"])

    def add_exception_event(
        self, exception: Exception, frames_infos: List[inspect.FrameInfo]
    ) -> None:
        if self.function_span:
            message = exception.args[0] if exception.args else None
            if not isinstance(message, str):
                message = str(message)
            self.function_span["error"] = {
                "type": exception.__class__.__name__,
                "message": message,
                "stacktrace": traceback.format_exc(),
                "frames": format_frames(frames_infos) if Configuration.verbose else [],
            }

    def add_step_end_event(self, ret_val):
        message_id = str(uuid.uuid4())
        step_function_span = StepFunctionParser().create_span(message_id)
        self.http_spans.append(recursive_json_join(step_function_span, self.base_msg))
        self.http_span_ids_to_send.add(step_function_span["id"])
        if isinstance(ret_val, dict):
            ret_val[LUMIGO_EVENT_KEY] = {STEP_FUNCTION_UID_KEY: message_id}
            get_logger().debug(f"Added key {LUMIGO_EVENT_KEY} to the user's return value")

    def get_tags_len(self) -> int:
        return len(self.function_span[EXECUTION_TAGS_KEY])

    def add_tag(self, key: str, value: str) -> None:
        self.function_span[EXECUTION_TAGS_KEY].append({"key": key, "value": value})

    def end(self, ret_val=None) -> Optional[int]:
        TimeoutMechanism.stop()
        reported_rtt = None
        self.previous_request = None, b""
        self.function_span.update({"ended": int(time.time() * 1000)})
        if Configuration.is_step_function:
            self.add_step_end_event(ret_val)
        if Configuration.verbose:
            self.function_span.update({"return_value": prepare_large_data(omit_keys(ret_val))})
        spans_contain_errors: bool = any(
            _is_span_has_error(s) for s in self.http_spans + [self.function_span]
        )

        if (not Configuration.send_only_if_error) or spans_contain_errors:
            to_send = [self.function_span] + [
                s for s in self.http_spans if s["id"] in self.http_span_ids_to_send
            ]
            reported_rtt = utils.report_json(region=self.region, msgs=to_send)
        else:
            get_logger().debug(
                "No Spans were sent, `Configuration.send_only_if_error` is on and no span has error"
            )
        return reported_rtt

    def get_patched_root(self):
        root = safe_split_get(self.trace_root, "-", 0)
        return f"Root={root}-0000{os.urandom(2).hex()}-{self.transaction_id}{self.trace_id_suffix}"

    @classmethod
    def get_span(cls) -> "SpansContainer":
        if cls._span:
            return cls._span
        return cls.create_span()

    @classmethod
    def create_span(cls, event=None, context=None, force=False) -> "SpansContainer":
        """
        This function creates a span out of a given AWS context.
        The force flag delete any existing span-container (to handle with warm execution of lambdas).
        Note that if lambda will be executed directly (regular pythonic function call and not invoked),
            it will override the container.
        """
        if cls._span and not force:
            return cls._span
        additional_info = {}
        if Configuration.verbose:
            additional_info.update(
                {
                    "event": prepare_large_data(EventParser.parse_event(omit_keys(event))),
                    "envs": prepare_large_data(omit_keys(dict(os.environ))),
                }
            )

        trace_root, transaction_id, suffix = parse_trace_id(os.environ.get("_X_AMZN_TRACE_ID", ""))
        remaining_time = getattr(context, "get_remaining_time_in_millis", lambda: MAX_LAMBDA_TIME)()
        cls._span = SpansContainer(
            started=int(time.time() * 1000),
            name=os.environ.get("AWS_LAMBDA_FUNCTION_NAME"),
            runtime=os.environ.get("AWS_EXECUTION_ENV"),
            region=os.environ.get("AWS_REGION"),
            memory_allocated=os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE"),
            log_stream_name=os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME"),
            log_group_name=os.environ.get("AWS_LAMBDA_LOG_GROUP_NAME"),
            trace_root=trace_root,
            transaction_id=transaction_id,
            trace_id_suffix=suffix,
            request_id=getattr(context, "aws_request_id", ""),
            account=safe_split_get(getattr(context, "invoked_function_arn", ""), ":", 4, ""),
            trigger_by=parse_triggered_by(event),
            max_finish_time=int(time.time() * 1000) + remaining_time,
            **additional_info,
        )
        return cls._span


class TimeoutMechanism:
    @staticmethod
    def start(seconds: int, to_exec: Callable):
        if Configuration.timeout_timer:
            signal.signal(signal.SIGALRM, to_exec)
            signal.setitimer(signal.ITIMER_REAL, seconds)

    @staticmethod
    def stop():
        if Configuration.timeout_timer:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, signal.SIG_DFL)

    @staticmethod
    def is_activated():
        return Configuration.timeout_timer and signal.getsignal(signal.SIGALRM) != signal.SIG_DFL
