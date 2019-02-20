from typing import List, Dict, Union

from lumigo_tracer import reporter
from lumigo_tracer.parsers.parser import get_parser
from lumigo_tracer.parsers.utils import parse_trace_id, safe_split_get
import time
import os

_VERSION_PATH = os.path.join(os.path.dirname(__file__), "..", "VERSION")


class EventType:
    RESPONSE = 1
    REQUEST = 2


class Span:
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
    ):
        self.name = name
        self.events: List[Dict[str, Union[Dict, None, str, int]]] = []
        version = open(_VERSION_PATH, "r").read() if os.path.exists(_VERSION_PATH) else "unknown"
        self.region = region
        self.trace_root = trace_root
        self.trace_id_suffix = trace_id_suffix
        # TODO - we omitted details - cold/warm etc.
        self.base_msg = {
            "started": started,
            "transactionId": transaction_id,
            "account": account,
            "region": region,
            "id": request_id,
            "info": {"tracer": {"version": version}, "traceId": {"Root": trace_root}},
        }
        start_msg = {
            **self.base_msg,
            **{
                "type": "function",
                "name": name,
                "runtime": runtime,
                "memoryAllocated": memory_allocated,
                "readiness": "warm",
            },
        }
        start_msg["info"].update(  # type: ignore
            {"logStreamName": log_stream_name, "logGroupName": log_group_name}
        )
        self.events.append(start_msg)

    def add_event(self, url: str, headers, body: bytes, event_type: EventType) -> None:
        """
        This function parses an input event and add it to the span.
        """
        parser = get_parser(url)()
        if event_type == EventType.REQUEST:
            msg = parser.parse_request(url, headers, body)
        else:
            msg = parser.parse_response(url, headers, body)
        self.events.append({**self.base_msg, **msg})

    def update_event(self, headers, body: str):
        # TODO connect it to the response event
        pass

    def add_exception_event(self, exception: Exception) -> None:
        if self.events:
            self.events[0].update({"error": f"{exception.__class__.__name__}: {exception.args[0]}"})

    def end(self) -> None:
        self.events[0].update({"ended": int(time.time() * 1000)})
        reporter.report_json(region=self.region, msgs=self.events[:])

    def get_patched_root(self):
        parts = self.trace_root.split("-", 2)
        return f"Root={parts[0]}-0000{os.urandom(2).hex()}-{parts[2]}{self.trace_id_suffix}"

    @classmethod
    def get_span(cls):
        if not cls._span:
            cls.create_span(None)
        return cls._span

    @classmethod
    def create_span(cls, context, force=False) -> None:
        """
        This function creates a span out of a given AWS context.
        """
        if cls._span and not force:
            return
        trace_root, transaction_id, suffix = parse_trace_id(os.environ.get("_X_AMZN_TRACE_ID", ""))
        cls._span = Span(
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
        )
