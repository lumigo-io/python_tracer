import os
import time
import uuid
import traceback
import http.client
from typing import List, Dict, Tuple, Optional

from lumigo_tracer.utils import Configuration, LUMIGO_EVENT_KEY, STEP_FUNCTION_UID_KEY
from lumigo_tracer import utils
from lumigo_tracer.parsers.parser import get_parser, HTTP_TYPE, StepFunctionParser
from lumigo_tracer.parsers.utils import (
    parse_trace_id,
    safe_split_get,
    recursive_json_join,
    parse_triggered_by,
    prepare_large_data,
)
from lumigo_tracer.utils import get_logger, _is_span_has_error
from .parsers.http_data_classes import HttpRequest
from lumigo_tracer.version import version

SEND_ONLY_IF_ERROR: bool = os.environ.get("SEND_ONLY_IF_ERROR", "").lower() == "true"
MAX_LAMBDA_TIME = 15 * 60 * 1000
MAX_BODY_SIZE = 1024


class SpansContainer:
    is_cold = True
    _span = None

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
        self.name = name
        self.events: List[Dict] = []
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
        self.start_msg = recursive_json_join(
            self.base_msg,
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
            },
        )
        self.previous_request: Tuple[Optional[http.client.HTTPMessage], bytes] = (None, b"")
        self.previous_response_body: bytes = b""
        SpansContainer.is_cold = False

    def start(self):
        to_send = self.start_msg.copy()
        to_send["id"] = f"{to_send['id']}_started"
        to_send["ended"] = to_send["started"]
        to_send["maxFinishTime"] = self.max_finish_time
        if not SEND_ONLY_IF_ERROR:
            report_duration = utils.report_json(region=self.region, msgs=[to_send])
            self.start_msg["reporter_rtt"] = report_duration
        else:
            get_logger().debug("Skip sending start because tracer in 'send only if error' mode .")
        self.events = [self.start_msg]

    def add_request_event(self, parse_params: HttpRequest):
        """
            This function parses an request event and add it to the span.
        """
        parser = get_parser(parse_params.host)()
        msg = parser.parse_request(parse_params)
        self.previous_request = parse_params.headers, parse_params.body
        self.events.append(recursive_json_join(self.base_msg, msg))

    def add_unparsed_request(self, parse_params: HttpRequest):
        """
        This function handle the case where we got a request the is not fully formatted as we expected,
        I.e. there isn't '\r\n' in the request data that <i>logically</i> splits the headers from the body.

        In that case, we will consider it as a continuance of the previous request if they got the same url,
            and we didn't get any answer yet.
        """
        if self.events:
            last_event = self.events[-1]
            if last_event and last_event.get("type") == HTTP_TYPE:
                if last_event.get("info", {}).get("httpInfo", {}).get("host") == parse_params.host:
                    if "response" not in last_event["info"]["httpInfo"]:
                        self.events.pop()
                        prev_headers, prev_body = self.previous_request
                        body = (prev_body + parse_params.body)[:MAX_BODY_SIZE]
                        self.add_request_event(parse_params.clone(headers=prev_headers, body=body))
                        return
        self.add_request_event(parse_params.clone(headers=None))

    def update_event_end_time(self) -> None:
        """
        This function assumes synchronous execution - we update the last http event.
        """
        if self.events:
            self.events[-1]["ended"] = int(time.time() * 1000)

    def update_event_response(
        self, host: Optional[str], status_code: int, headers: http.client.HTTPMessage, body: bytes
    ) -> None:
        """
        :param host: If None, use the host from the last span, otherwise this is the first chuck and we can empty
                            the aggregated response body
        This function assumes synchronous execution - we update the last http event.
        """
        if self.events:
            last_event = self.events.pop()
            if not host:
                host = last_event.get("info", {}).get("httpInfo", {}).get("host", "unknown")
            else:
                self.previous_response_body = b""

            parser = get_parser(host)()  # type: ignore
            self.previous_response_body += body
            self.events.append(
                recursive_json_join(
                    parser.parse_response(  # type: ignore
                        host, status_code, headers, self.previous_response_body
                    ),
                    last_event,
                )
            )

    def add_exception_event(self, exception: Exception) -> None:
        if self.events:
            msg = {
                "type": exception.__class__.__name__,
                "message": exception.args[0] if exception.args else None,
                "stacktrace": traceback.format_exc(),
            }
            self.events[0].update({"error": msg})

    def add_step_end_event(self, ret_val):
        message_id = str(uuid.uuid4())
        step_function_span = StepFunctionParser().create_span(message_id)
        self.events.append(recursive_json_join(self.base_msg, step_function_span))
        if isinstance(ret_val, dict):
            ret_val[LUMIGO_EVENT_KEY] = {STEP_FUNCTION_UID_KEY: message_id}
            get_logger().debug(f"Added key {LUMIGO_EVENT_KEY} to the user's return value")

    def end(self, ret_val) -> Optional[int]:
        reported_rtt = None
        self.previous_request = None, b""
        self.events[0].update({"ended": int(time.time() * 1000)})
        if Configuration.is_step_function:
            self.add_step_end_event(ret_val)
        if Configuration.verbose:
            self.events[0].update({"return_value": prepare_large_data(ret_val)})
        spans_contain_errors: bool = any(_is_span_has_error(s) for s in self.events)

        if (not SEND_ONLY_IF_ERROR) or spans_contain_errors:
            reported_rtt = utils.report_json(region=self.region, msgs=self.events[:])
        else:
            get_logger().debug(
                "No Spans were sent, `SEND_ONLY_IF_ERROR` is on and no span has error"
            )
        return reported_rtt

    def get_patched_root(self):
        root = safe_split_get(self.trace_root, "-", 0)
        return f"Root={root}-0000{os.urandom(2).hex()}-{self.transaction_id}{self.trace_id_suffix}"

    @classmethod
    def get_span(cls):
        if not cls._span:
            cls.create_span()
        return cls._span

    @classmethod
    def create_span(cls, event=None, context=None, force=False) -> None:
        """
        This function creates a span out of a given AWS context.
        The force flag delete any existing span-container (to handle with warm execution of lambdas).
        Note that if lambda will be executed directly (regular pythonic function call and not invoked),
            it will override the container.
        """
        if cls._span and not force:
            return
        additional_info = {}
        if Configuration.verbose:
            additional_info.update(
                {"event": prepare_large_data(event), "envs": prepare_large_data(dict(os.environ))}
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
