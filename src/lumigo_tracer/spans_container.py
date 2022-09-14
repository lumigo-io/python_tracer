import copy
import inspect
import os
from datetime import datetime

import time
import uuid
import signal
from typing import List, Dict, Optional, Callable, Set, Union

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
    get_current_ms_time,
    get_region,
    is_provision_concurrency_initialization,
    get_stacktrace,
    write_extension_file,
    should_use_tracer_extension,
    MANUAL_TRACES_KEY,
    lumigo_safe_execute,
)
from lumigo_tracer import lumigo_utils
from lumigo_tracer.parsing_utils import parse_trace_id, safe_split_get, recursive_json_join
from lumigo_tracer.event.event_trigger import parse_triggered_by

_VERSION_PATH = os.path.join(os.path.dirname(__file__), "VERSION")
MAX_LAMBDA_TIME = 15 * 60 * 1000
FUNCTION_TYPE = "function"
ENRICHMENT_TYPE = "enrichment"
MALFORMED_TXID = "000000000000000000000000"


class SpansContainer:
    lambda_container_id = str(uuid.uuid4())
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
        trigger_by: dict = None,  # type: ignore[type-arg]
        max_finish_time: int = None,
        is_new_invocation: bool = False,
        event: str = None,
        envs: str = None,
    ):
        version = open(_VERSION_PATH, "r").read() if os.path.exists(_VERSION_PATH) else "unknown"
        version = version.strip()
        self.name = name
        self.region = region
        self.trace_root = trace_root
        self.trace_id_suffix = trace_id_suffix
        malformed_txid = False
        if transaction_id == MALFORMED_TXID:
            transaction_id = os.urandom(12).hex()
            malformed_txid = True
        self.transaction_id = transaction_id
        self.max_finish_time = max_finish_time
        self.base_msg = {
            "lambda_container_id": SpansContainer.lambda_container_id,
            "started": started,
            "transactionId": transaction_id,
            "account": account,
            "region": region,
            "parentId": request_id,
            "info": {"tracer": {"version": version}, "traceId": {"Root": trace_root}},
            "token": Configuration.token,
        }
        is_cold = SpansContainer.is_cold and not is_provision_concurrency_initialization()
        self.function_span = recursive_json_join(
            {
                "id": request_id,
                "type": FUNCTION_TYPE,
                "name": name,
                "runtime": runtime,
                "event": event,
                "envs": envs,
                "memoryAllocated": memory_allocated,
                "readiness": "cold" if is_cold else "warm",
                "info": {
                    "logStreamName": log_stream_name,
                    "logGroupName": log_group_name,
                    **(trigger_by or {}),
                },
                "isMalformedTransactionId": malformed_txid,
                MANUAL_TRACES_KEY: [],
            },
            self.base_msg,
        )
        self.base_enrichment_span = {
            "type": ENRICHMENT_TYPE,
            "token": Configuration.token,
            "invocation_id": request_id,
            "transaction_id": transaction_id,
        }
        self.execution_tags: List[Dict[str, str]] = []
        self.span_ids_to_send: Set[str] = set()
        self.spans: Dict[str, Dict] = {}  # type: ignore[type-arg]
        self.manual_trace_start_times: Dict[str, int] = {}
        if is_new_invocation:
            SpansContainer.is_cold = False

    def _generate_start_span(self) -> dict:  # type: ignore[type-arg]
        to_send = self.function_span.copy()
        to_send["id"] = f"{to_send['id']}_started"
        to_send["ended"] = to_send["started"]
        to_send["maxFinishTime"] = self.max_finish_time
        return to_send  # type: ignore[no-any-return]

    def generate_enrichment_span(self) -> Optional[Dict[str, Union[str, int]]]:
        if not self.execution_tags:
            return None
        return recursive_json_join(  # type: ignore[no-any-return]
            {"sending_time": get_current_ms_time(), EXECUTION_TAGS_KEY: self.execution_tags.copy()},
            self.base_enrichment_span,
        )

    def start(self, event=None, context=None):  # type: ignore[no-untyped-def]
        to_send = self._generate_start_span()
        if not Configuration.send_only_if_error:
            report_duration = lumigo_utils.report_json(
                region=self.region, msgs=[to_send], is_start_span=True
            )
            self.function_span["reporter_rtt"] = report_duration
        else:
            get_logger().debug("Skip sending start because tracer in 'send only if error' mode .")
        self.start_timeout_timer(context)

    def handle_timeout(self, *args):  # type: ignore[no-untyped-def]
        with lumigo_safe_execute("spans container: handle_timeout"):
            get_logger().info("The tracer reached the end of the timeout timer")
            spans_id_copy = self.span_ids_to_send.copy()
            to_send = [self.spans[span_id] for span_id in spans_id_copy]
            enrichment_span = self.generate_enrichment_span()
            if enrichment_span:
                to_send.insert(0, enrichment_span)
            self.span_ids_to_send.clear()
            if Configuration.send_only_if_error:
                to_send.append(self._generate_start_span())
            lumigo_utils.report_json(region=self.region, msgs=to_send)

    def start_timeout_timer(self, context=None) -> None:  # type: ignore[no-untyped-def]
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

    def add_span(self, span: dict) -> dict:  # type: ignore[type-arg]
        """
        This function parses an request event and add it to the span.
        """
        new_span = recursive_json_join(span, self.base_msg)
        span_id = new_span["id"]
        self.spans[span_id] = new_span
        self.span_ids_to_send.add(span_id)
        return new_span  # type: ignore[no-any-return]

    def get_span_by_id(self, span_id: Optional[str]) -> Optional[dict]:  # type: ignore[type-arg]
        if not span_id:
            return None
        return self.spans.get(span_id)

    def pop_span(self, span_id: Optional[str]) -> Optional[dict]:  # type: ignore[type-arg]
        if not span_id:
            return None
        self.span_ids_to_send.discard(span_id)
        return self.spans.pop(span_id, None)

    def update_event_end_time(self, span_id: str) -> None:
        """
        This function assumes synchronous execution - we update the last http event.
        """
        if span_id in self.spans:
            self.spans[span_id]["ended"] = get_current_ms_time()
            self.span_ids_to_send.add(span_id)
        else:
            get_logger().warning(f"update_event_end_time: Got unknown span id: {span_id}")

    def update_event_times(
        self,
        span_id: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> None:
        """
        This function assumes synchronous execution - we update the last http event.
        """
        if span_id in self.spans:
            start_timestamp = start_time.timestamp() if start_time else time.time()
            self.spans[span_id]["started"] = int(start_timestamp * 1000)
            if end_time:
                self.spans[span_id]["ended"] = end_time.timestamp()
        else:
            get_logger().warning(f"update_event_times: Got unknown span id: {span_id}")

    @staticmethod
    def _create_exception_event(  # type: ignore[no-untyped-def]
        exc_type: str, message: str, stacktrace: str = "", frames: Optional[List[dict]] = None  # type: ignore[type-arg]
    ):
        return {
            "type": exc_type,
            "message": message,
            "stacktrace": stacktrace,
            "frames": frames or [],
        }

    @staticmethod
    def add_exception_to_span(  # type: ignore[no-untyped-def]
        span: dict, exception: Exception, frames_infos: List[inspect.FrameInfo]  # type: ignore[type-arg]
    ):
        message = exception.args[0] if exception.args else None
        if not isinstance(message, str):
            message = str(message)
        span["error"] = SpansContainer._create_exception_event(
            exc_type=exception.__class__.__name__,
            message=message,
            stacktrace=get_stacktrace(exception),
            frames=format_frames(frames_infos) if Configuration.verbose else [],
        )

    def add_exception_event(
        self, exception: Exception, frames_infos: List[inspect.FrameInfo]
    ) -> None:
        if self.function_span:
            self.add_exception_to_span(self.function_span, exception, frames_infos)

    def add_step_end_event(self, ret_val):  # type: ignore[no-untyped-def]
        message_id = str(uuid.uuid4())
        step_function_span = create_step_function_span(message_id)
        span_id = step_function_span["id"]
        self.spans[span_id] = recursive_json_join(step_function_span, self.base_msg)
        self.span_ids_to_send.add(span_id)
        if isinstance(ret_val, dict):
            ret_val[LUMIGO_EVENT_KEY] = {STEP_FUNCTION_UID_KEY: message_id}
            get_logger().debug(f"Added key {LUMIGO_EVENT_KEY} to the user's return value")

    def get_tags_len(self) -> int:
        return len(self.execution_tags)

    def add_tag(self, key: str, value: str) -> None:
        self.execution_tags.append({"key": key, "value": value})

    def start_manual_trace(self, name: str) -> None:
        now = get_current_ms_time()
        self.manual_trace_start_times[name] = now

    def stop_manual_trace(self, name: str) -> None:
        manual_trace_started = self.manual_trace_start_times.pop(name, None)
        if manual_trace_started:
            now = get_current_ms_time()
            self.function_span[MANUAL_TRACES_KEY].append(
                {"name": name, "startTime": manual_trace_started, "endTime": now}
            )

    def end(self, ret_val=None, event: Optional[dict] = None, context=None) -> Optional[int]:  # type: ignore[no-untyped-def,type-arg]
        TimeoutMechanism.stop()
        reported_rtt = None
        self.previous_request = None
        self.function_span.update({"ended": get_current_ms_time()})
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
        if _is_span_has_error(self.function_span):
            self._set_error_extra_data(event)
        spans_contain_errors: bool = any(
            _is_span_has_error(s) for s in self.spans.values()
        ) or _is_span_has_error(self.function_span)

        if (not Configuration.send_only_if_error) or spans_contain_errors:
            to_send = [self.function_span] + [
                span for span_id, span in self.spans.items() if span_id in self.span_ids_to_send
            ]
            enrichment_span = self.generate_enrichment_span()
            if enrichment_span:
                to_send.append(enrichment_span)
            reported_rtt = lumigo_utils.report_json(region=self.region, msgs=to_send)
        else:
            get_logger().debug(
                "No Spans were sent, `Configuration.send_only_if_error` is on and no span has error"
            )
            if should_use_tracer_extension():
                write_extension_file([{}], "stop")
        return reported_rtt

    def _set_error_extra_data(self, event):  # type: ignore[no-untyped-def]
        self.function_span["envs"] = _get_envs_for_span(has_error=True)
        if event:
            self.function_span["event"] = EventDumper.dump_event(
                copy.deepcopy(event), has_error=True
            )

    def can_path_root(self):  # type: ignore[no-untyped-def]
        return self.trace_root and self.transaction_id and self.trace_id_suffix

    def get_patched_root(self):  # type: ignore[no-untyped-def]
        """
        We're changing the root in order to pass/share the transaction id. More info:
        https://docs.aws.amazon.com/xray/latest/devguide/xray-api-sendingdata.html#xray-api-traceids
        """
        current_time = int(time.time())
        root = safe_split_get(self.trace_root, "-", 0)  # type: ignore[arg-type]
        return f"Root={root}-{hex(current_time)[2:]}-{self.transaction_id}{self.trace_id_suffix}"

    @classmethod
    def get_span(cls) -> "SpansContainer":
        if cls._span:
            return cls._span
        return cls.create_span()

    @classmethod
    def create_span(cls, event=None, context=None, is_new_invocation=False) -> "SpansContainer":  # type: ignore[no-untyped-def]
        """
        This function creates a span out of a given AWS context.
        The force flag delete any existing span-container (to handle with warm execution of lambdas).
        Note that if lambda will be executed directly (regular pythonic function call and not invoked),
            it will override the container.
        """
        if cls._span and not is_new_invocation:
            return cls._span
        # copy the event to ensure that we will not change it
        event = copy.deepcopy(event)
        additional_info = {}
        if Configuration.verbose:
            additional_info.update(
                {"event": EventDumper.dump_event(event), "envs": _get_envs_for_span()}
            )

        trace_root, transaction_id, suffix = parse_trace_id(os.environ.get("_X_AMZN_TRACE_ID", ""))
        remaining_time = getattr(context, "get_remaining_time_in_millis", lambda: MAX_LAMBDA_TIME)()
        cls._span = SpansContainer(
            started=get_current_ms_time(),
            name=os.environ.get("AWS_LAMBDA_FUNCTION_NAME"),
            runtime=os.environ.get("AWS_EXECUTION_ENV"),
            region=get_region(),
            memory_allocated=os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE"),
            log_stream_name=os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME"),
            log_group_name=os.environ.get("AWS_LAMBDA_LOG_GROUP_NAME"),
            trace_root=trace_root,
            transaction_id=transaction_id,
            trace_id_suffix=suffix,
            request_id=getattr(context, "aws_request_id", ""),
            account=safe_split_get(getattr(context, "invoked_function_arn", ""), ":", 4, ""),
            trigger_by=parse_triggered_by(event),
            max_finish_time=get_current_ms_time() + remaining_time,
            is_new_invocation=is_new_invocation,
            **additional_info,
        )
        return cls._span


class TimeoutMechanism:
    @staticmethod
    def start(seconds: int, to_exec: Callable):  # type: ignore[no-untyped-def,type-arg]
        if Configuration.timeout_timer:
            signal.signal(signal.SIGALRM, to_exec)
            signal.setitimer(signal.ITIMER_REAL, seconds)

    @staticmethod
    def stop():  # type: ignore[no-untyped-def]
        if Configuration.timeout_timer:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, signal.SIG_DFL)

    @staticmethod
    def is_activated():  # type: ignore[no-untyped-def]
        return Configuration.timeout_timer and signal.getsignal(signal.SIGALRM) != signal.SIG_DFL


def _get_envs_for_span(has_error: bool = False) -> str:
    return lumigo_dumps(dict(os.environ), Configuration.get_max_entry_size(has_error))
