import json
from typing import Dict, Optional

from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.lumigo_utils import Configuration, warn_client

LUMIGO_REPORT_ERROR_STRING = "[LUMIGO_LOG]"
MAX_TAGS = 50
MAX_ELEMENTS_IN_EXTRA = 10
MAX_TAG_KEY_LEN = MAX_TAG_VALUE_LEN = 50
ADD_TAG_ERROR_MSG_PREFIX = "Skipping add_execution_tag: Unable to add tag"


def info(msg: str, error_type: str = "ProgrammaticInfo", extra: Dict[str, str] = None):
    log(20, msg, error_type, extra)


def warn(msg: str, error_type: str = "ProgrammaticWarn", extra: Dict[str, str] = None):
    log(30, msg, error_type, extra)


def error(msg: str, error_type: str = "ProgrammaticError", extra: Dict[str, str] = None):
    log(40, msg, error_type, extra)


def log(level: int, msg: str, error_type: str, extra: Optional[Dict[str, str]]):
    tags_len = SpansContainer.get_span().get_tags_len()
    extra_filtered = list(
        filter(
            lambda element: validate_tag(element[0], str(element[1]), tags_len, True),
            (extra or {}).items(),
        )
    )
    actual = {key: str(value) for key, value in extra_filtered[:MAX_ELEMENTS_IN_EXTRA]}
    text = json.dumps({"message": msg, "type": error_type, "level": level, **actual})
    print(LUMIGO_REPORT_ERROR_STRING, text)


def report_error(msg: str):
    message_with_initials = f"{LUMIGO_REPORT_ERROR_STRING} {msg}"
    if Configuration.enhanced_print:
        print(message_with_initials)
    else:
        message_with_request_id = f"RequestId: {SpansContainer.get_span().function_span.get('id')} {message_with_initials}"
        print(message_with_request_id)


def validate_tag(key, value, tags_len, should_log_errors):
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

    return True


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
        key = str(key)
        value = str(value)
        tags_len = SpansContainer.get_span().get_tags_len()
        if validate_tag(key, value, tags_len, should_log_errors):
            SpansContainer.get_span().add_tag(key, value)
        else:
            return False
    except Exception:
        if should_log_errors:
            warn_client(ADD_TAG_ERROR_MSG_PREFIX)
        return False
    return True
