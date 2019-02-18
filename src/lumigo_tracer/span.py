from lumigo_tracer import reporter
from lumigo_tracer.parsers.parser import get_parser
from lumigo_tracer.parsers.utils import parse_trace_id, safe_split_get
import time
import os


class EventType:
    RESPONSE = 1
    REQUEST = 2


class Span(object):
    _span = None

    def __init__(
        self,
        name=None,
        started=None,
        region=None,
        runtime=None,
        memory_allocated=None,
        log_stream_name=None,
        log_group_name=None,
        trace_root=None,
        transaction_id=None,
        request_id=None,
        account=None,
    ):
        self.name = name
        self.events = []
        version = "0.1"  # TODO use version file
        # TODO - we omitted details - cold/hold etc.
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
            },
        }
        start_msg["info"].update({"logStreamName": log_stream_name, "logGroupName": log_group_name})
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
        # TODO should we update the first event or send a new one?
        self.events.append(
            {"exception_name": exception.__class__.__name__, "exception_message": exception.args[0]}
        )

    def end(self) -> None:
        self.events.append({"ended": time.time()})
        for event in self.events[:]:
            reporter.report_json(event)

    @classmethod
    def get_span(cls):
        if not cls._span:
            cls.create_span(None)
        return cls._span

    @classmethod
    def create_span(cls, context):
        trace_root, transaction_id = parse_trace_id(os.environ.get("_X_AMZN_TRACE_ID", ""))
        cls._span = Span(
            started=time.time(),
            name=os.environ.get("AWS_LAMBDA_FUNCTION_NAME"),
            runtime=os.environ.get("AWS_EXECUTION_ENV"),
            region=os.environ.get("AWS_REGION"),
            memory_allocated=os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE"),
            log_stream_name=os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME"),
            log_group_name=os.environ.get("AWS_LAMBDA_LOG_GROUP_NAME"),
            trace_root=trace_root,
            transaction_id=transaction_id,
            request_id=getattr(context, "aws_request_id", ""),
            account=safe_split_get(getattr(context, "invoked_function_arn", ""), ":", 4, ""),
        )
