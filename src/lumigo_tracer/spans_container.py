import copy
import inspect
import os
from datetime import datetime

import time
import uuid
import signal
import traceback
from typing import List, Dict, Optional, Callable, Set

from lumigo_tracer.event.event_dumper import EventDumper
from lumigo_tracer.lumigo_utils import (
    Configuration,
    LUMIGO_EVENT_KEY,
    STEP_FUNCTION_UID_KEY,
    format_frames,
    lumigo_dumps,
    EXECUTION_TAGS_KEY,
    get_timeout_buffer,
    get_logger,
    _is_span_has_error,
    create_step_function_span,
)
from lumigo_tracer import lumigo_utils
from lumigo_tracer.parsing_utils import parse_trace_id, safe_split_get, recursive_json_join
from lumigo_tracer.event.event_trigger import parse_triggered_by

_VERSION_PATH = os.path.join(os.path.dirname(__file__), "VERSION")
MAX_LAMBDA_TIME = 15 * 60 * 1000
MAX_BODY_SIZE = 1024
FUNCTION_TYPE = "function"


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
            "token": Configuration.token,
        }
        self.function_span = recursive_json_join(
            {
                "id": request_id,
                "type": FUNCTION_TYPE,
                "name": name,
                "runtime": runtime,
                "event": event,
                "envs": envs,
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
        self.span_ids_to_send: Set[str] = set()
        self.spans: List[Dict] = []
        SpansContainer.is_cold = False

    def _generate_start_span(self) -> dict:
        to_send = self.function_span.copy()
        to_send["id"] = f"{to_send['id']}_started"
        to_send["ended"] = to_send["started"]
        to_send["maxFinishTime"] = self.max_finish_time
        return to_send

    def start(self, event=None, context=None):
        to_send = self._generate_start_span()
        if not Configuration.send_only_if_error:
            report_duration = lumigo_utils.report_json(region=self.region, msgs=[to_send])
            self.function_span["reporter_rtt"] = report_duration
            self.spans = []
        else:
            get_logger().debug("Skip sending start because tracer in 'send only if error' mode .")
        self.start_timeout_timer(context)

    def handle_timeout(self, *args):
        get_logger().info("The tracer reached the end of the timeout timer")
        to_send = [s for s in self.spans if s["id"] in self.span_ids_to_send]
        if Configuration.send_only_if_error:
            to_send.append(self._generate_start_span())
        lumigo_utils.report_json(region=self.region, msgs=to_send)
        self.span_ids_to_send.clear()

    def start_timeout_timer(self, context=None) -> None:
        if Configuration.timeout_timer:
            if not hasattr(context, "get_remaining_time_in_millis"):
                get_logger().info("Skip setting timeout timer - Could not get the remaining time.")
                return
            remaining_time = context.get_remaining_time_in_millis() / 1000
            buffer = get_timeout_buffer(remaining_time)
            if buffer >= remaining_time or remaining_time < 2:
                get_logger().debug("Skip setting timeout timer - Too short timeout.")
                return
            TimeoutMechanism.start(remaining_time - buffer, self.handle_timeout)

    def add_span(self, span: dict):
        """
        This function parses an request event and add it to the span.
        """
        self.spans.append(recursive_json_join(span, self.base_msg))
        self.span_ids_to_send.add(span["id"])

    def get_last_span(self) -> Optional[dict]:
        if not self.spans:
            return None
        return self.spans[-1]

    def remove_last_span(self) -> Optional[dict]:
        return self.spans.pop() if self.spans else None

    def update_event_end_time(self) -> None:
        """
        This function assumes synchronous execution - we update the last http event.
        """
        if self.spans:
            self.spans[-1]["ended"] = int(time.time() * 1000)

    def update_event_times(
        self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None
    ) -> None:
        """
        This function assumes synchronous execution - we update the last http event.
        """
        if self.spans:
            start_timestamp = start_time.timestamp() if start_time else time.time()
            end_timestamp = end_time.timestamp() if end_time else time.time()
            self.spans[-1]["started"] = int(start_timestamp * 1000)
            self.spans[-1]["ended"] = int(end_timestamp * 1000)

    def _create_exception_event(
        self, exc_type: str, message: str, stacktrace: str = "", frames: Optional[List[dict]] = None
    ):
        return {
            "type": exc_type,
            "message": message,
            "stacktrace": stacktrace,
            "frames": frames or [],
        }

    def add_exception_event(
        self, exception: Exception, frames_infos: List[inspect.FrameInfo]
    ) -> None:
        if self.function_span:
            message = exception.args[0] if exception.args else None
            if not isinstance(message, str):
                message = str(message)
            self.function_span["error"] = self._create_exception_event(
                exc_type=exception.__class__.__name__,
                message=message,
                stacktrace=traceback.format_exc(),
                frames=format_frames(frames_infos) if Configuration.verbose else [],
            )

    def add_step_end_event(self, ret_val):
        message_id = str(uuid.uuid4())
        step_function_span = create_step_function_span(message_id)
        self.spans.append(recursive_json_join(step_function_span, self.base_msg))
        self.span_ids_to_send.add(step_function_span["id"])
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
        self.previous_request = None
        self.function_span.update({"ended": int(time.time() * 1000)})
        if Configuration.is_step_function:
            self.add_step_end_event(ret_val)
        parsed_ret_val = None
        if Configuration.verbose:
            try:
                if ret_val is not None:
                    parsed_ret_val = lumigo_dumps(ret_val, enforce_jsonify=True, decimal_safe=True)
            except Exception as err:
                suffix = ""
                if err.args:
                    suffix = f'Original message: "{err.args[0]}"'
                self.function_span["error"] = self._create_exception_event(
                    "ReturnValueError",
                    "The lambda will probably fail due to bad return value. " + suffix,
                )
        self.function_span.update({"return_value": parsed_ret_val})
        spans_contain_errors: bool = any(
            _is_span_has_error(s) for s in self.spans + [self.function_span]
        )

        if (not Configuration.send_only_if_error) or spans_contain_errors:
            to_send = [self.function_span] + [
                s for s in self.spans if s["id"] in self.span_ids_to_send
            ]
            reported_rtt = lumigo_utils.report_json(region=self.region, msgs=to_send)
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
        # copy the event to ensure that we will not change it
        event = copy.deepcopy(event)
        additional_info = {}
        if Configuration.verbose:
            additional_info.update(
                {"event": EventDumper.dump_event(event), "envs": lumigo_dumps(dict(os.environ))}
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
