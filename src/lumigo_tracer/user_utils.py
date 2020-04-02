from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.utils import Configuration, warn_client

LUMIGO_REPORT_ERROR_STRING = "[LUMIGO_LOG]"
MAX_TAGS = 50
MAX_TAG_KEY_LEN = MAX_TAG_VALUE_LEN = 50
ADD_TAG_ERROR_MSG_PREFIX = "Skipping add_execution_tag: Unable to add tag"


def report_error(msg: str):
    message_with_initials = f"{LUMIGO_REPORT_ERROR_STRING} {msg}"
    if Configuration.enhanced_print:
        print(message_with_initials)
    else:
        message_with_request_id = f"RequestId: {SpansContainer.get_span().function_span.get('id')} {message_with_initials}"
        print(message_with_request_id)


def add_execution_tag(key: str, value: str, should_log_errors: bool = True) -> bool:
    """
    Use this function to add an execution_tag to your function with a dynamic value.
    This value can be searched within the Lumigo platform.

    The maximum number of tags is 50.
    :param key: Length should be between 1 and 50.
    :param value: Length should be between 1 and 50.
    :param should_log_errors: Should a log message be printed in case the tag can't be added.
    """
    try:
        tags_len = SpansContainer.get_span().get_tags_len()
        key = str(key)
        value = str(value)
        if not key or len(key) >= MAX_TAG_KEY_LEN:
            if should_log_errors:
                warn_client(
                    f"{ADD_TAG_ERROR_MSG_PREFIX}: key length should be between 1 and {MAX_TAG_KEY_LEN}: {key} - {value}"
                )
            return False
        if not value or len(value) >= MAX_TAG_VALUE_LEN:
            if should_log_errors:
                warn_client(
                    f"{ADD_TAG_ERROR_MSG_PREFIX}: value length should be between 1 and {MAX_TAG_VALUE_LEN}: {key} - {value}"
                )
            return False
        if tags_len >= MAX_TAGS:
            if should_log_errors:
                warn_client(
                    f"{ADD_TAG_ERROR_MSG_PREFIX}: maximum number of tags is {MAX_TAGS}: {key} - {value}"
                )
            return False
        SpansContainer.get_span().add_tag(key, value)
    except Exception:
        if should_log_errors:
            warn_client(ADD_TAG_ERROR_MSG_PREFIX)
        return False
    return True
