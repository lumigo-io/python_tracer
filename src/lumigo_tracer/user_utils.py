from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.utils import Configuration

LUMIGO_REPORT_ERROR_STRING = "[LUMIGO_LOG]"


def report_error(msg: str):
    message_with_initials = f"{LUMIGO_REPORT_ERROR_STRING} {msg}"
    if Configuration.enhanced_print:
        print(message_with_initials)
    else:
        message_with_request_id = f"RequestId: {SpansContainer.get_span().function_span.get('id')} {message_with_initials}"
        print(message_with_request_id)
