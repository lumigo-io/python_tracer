from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.utils import Configuration, warn_client

LUMIGO_REPORT_ERROR_STRING = "[LUMIGO_LOG]"


def report_error(msg: str):
    message_with_initials = f"{LUMIGO_REPORT_ERROR_STRING} {msg}"
    if Configuration.enhanced_print:
        print(message_with_initials)
    else:
        message_with_request_id = f"RequestId: {SpansContainer.get_span().function_span.get('id')} {message_with_initials}"
        print(message_with_request_id)


def add_tag(key: str, value: str) -> None:
    """
    Max of 50 tags.
    :param key: A string longer than 0 and shorter than 50.
    :param value: A string longer than 0 and shorter than 50.
    """
    success = SpansContainer.get_span().add_tag(key, value)
    if not success:
        warn_client(f"Unable to add tag: {key} - {value}")
