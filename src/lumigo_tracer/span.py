from lumigo_tracer import reporter
from .parsers.parser import get_parser
import time


class EventType:
    RESPONSE = 1
    REQUEST = 2


class Span(object):
    _span = None

    def __init__(self, span_name):
        self.span_name = span_name
        self.events = []
        self.events.append({"name": span_name, "start_time": time.time()})

    def add_event(self, url: str, headers, body: bytes, event_type: EventType) -> None:
        """
        This function parses an input event and add it to the span.
        """
        parser = get_parser(url)()
        if event_type == EventType.REQUEST:
            msg = parser.parse_request(url, headers, body)
        else:
            msg = parser.parse_response(url, headers, body)
        self.events.append(msg)

    def add_exception_event(self, exception: Exception) -> None:
        self.events.append(
            {"exception_name": exception.__class__.__name__, "exception_message": exception.args[0]}
        )

    def end(self) -> None:
        self.events.append({"end_time": time.time()})
        for event in self.events:
            reporter.report_json(event)

    @classmethod
    def get_span(cls):
        if not cls._span:
            print("WARNING: called to wrapper before span initialized")
            Span.create_span("Unknown function")
        return cls._span

    @classmethod
    def create_span(cls, function_name):
        cls._span = Span(function_name)
