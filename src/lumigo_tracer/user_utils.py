import json
import logging
from typing import Dict, Optional

from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.lumigo_utils import Configuration, warn_client

LUMIGO_REPORT_ERROR_STRING = "[LUMIGO_LOG]"
MAX_TAGS = 50
MAX_ELEMENTS_IN_EXTRA = 10
MAX_TAG_KEY_LEN = 50
MAX_TAG_VALUE_LEN = 70
ADD_TAG_ERROR_MSG_PREFIX = "Skipping add_execution_tag: Unable to add tag"


def info(msg: str, alert_type: str = "ProgrammaticInfo", extra: Dict[str, str] = None):
    """
    Use this function to create a log entry in your lumigo platform.
    You can use it to dynamically generate alerts programmatically with searchable fields.
    Then use the lumigo explore to search and filters logs in free text.

    :param msg: a free text to log
    :param alert_type: Should be considered as a grouping parameter. This indicates the type of this message. Default: ProgrammaticInfo
    :param extra: a key-value dict. Limited to 10 keys and 50 characters per value.
    """
    log(logging.INFO, msg, alert_type, extra)


def warn(msg: str, alert_type: str = "ProgrammaticWarn", extra: Dict[str, str] = None):
    """
    Use this function to create a log entry in your lumigo platform.
    You can use it to dynamically generate alerts programmatically with searchable fields.
    Then use the lumigo explore to search and filters logs in free text.

    :param msg: a free text to log
    :param alert_type: Should be considered as a grouping parameter. This indicates the type of this message. Default: ProgrammaticWarn
    :param extra: a key-value dict. Limited to 10 keys and 50 characters per value.
    """
    log(logging.WARN, msg, alert_type, extra)


def error(
    msg: str,
    alert_type: Optional[str] = None,
    extra: Optional[Dict[str, str]] = None,
    err: Optional[Exception] = None,
):
    """
    Use this function to create a log entry in your lumigo platform.
    You can use it to dynamically generate alerts programmatically with searchable fields.
    Then use the lumigo explore to search and filters logs in free text.

    :param msg: a free text to log
    :param alert_type: Should be considered as a grouping parameter. This indicates the type of this message. Default: take the given exception type or ProgrammaticError if its None
    :param extra: a key-value dict. Limited to 10 keys and 50 characters per value. By default we're taking the excpetion raw message
    :param err: the actual error object.
    """

    extra = extra or {}
    if err:
        extra["raw_exception"] = str(err)
        alert_type = alert_type or err.__class__.__name__
    alert_type = alert_type or "ProgrammaticError"
    log(logging.ERROR, msg, alert_type, extra)


def log(level: int, msg: str, error_type: str, extra: Optional[Dict[str, str]]):
    filtered_extra = list(
        filter(
            lambda element: validate_tag(element[0], element[1], 0, True),
            (extra or {}).items(),
        )
    )
    extra = {key: str(value) for key, value in filtered_extra[:MAX_ELEMENTS_IN_EXTRA]}
    actual = {"message": msg, "type": error_type, "level": level}
    if extra:
        actual["extra"] = extra
    print(LUMIGO_REPORT_ERROR_STRING, json.dumps(actual))


def report_error(msg: str):
    message_with_initials = f"{LUMIGO_REPORT_ERROR_STRING} {msg}"
    if Configuration.enhanced_print:
        print(message_with_initials)
    else:
        message_with_request_id = f"RequestId: {SpansContainer.get_span().function_span.get('id')} {message_with_initials}"
        print(message_with_request_id)


def validate_tag(key, value, tags_len, should_log_errors):
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

    return True


def add_execution_tag(key: str, value: str, should_log_errors: bool = True) -> bool:
    """
    Use this function to add an execution_tag to your function with a dynamic value.
    This value can be searched within the Lumigo platform.

    The maximum number of tags is 50.
    :param key: Length should be between 1 and 50.
    :param value: Length should be between 1 and 70.
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
