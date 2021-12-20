from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.user_utils import (
    warn,
    info,
    error,
    LUMIGO_REPORT_ERROR_STRING,
    add_execution_tag,
    MAX_TAG_KEY_LEN,
    MAX_TAG_VALUE_LEN,
    MAX_TAGS,
    MAX_ELEMENTS_IN_EXTRA,
)
from lumigo_tracer.lumigo_utils import EXECUTION_TAGS_KEY


def test_err_without_alert_type_with_exception(capsys):
    msg = '{"message": "This is error message", "type": "RuntimeError", "level": 40, "extra": {"a": "3", "b": "True", "c": "aaa", "d": "{}", "aa": "a", "a0": "0", "a1": "1", "a2": "2", "a3": "3", "a4": "4"}}'
    error(
        err=RuntimeError("Failed to open database"),
        msg="This is error message",
        extra={
            "a": 3,
            "b": True,
            "c": "aaa",
            "d": {},
            "aa": "a",
            "A" * 100: "A" * 100,
            **{f"a{i}": i for i in range(MAX_ELEMENTS_IN_EXTRA)},
        },
    )
    captured = capsys.readouterr().out.split("\n")
    assert captured[1] == f"{LUMIGO_REPORT_ERROR_STRING} {msg}"


def test_err_with_type_and_exception(capsys):
    msg = (
        '{"message": "This is error message", "type": "DBError",'
        ' "level": 40, "extra": {"raw_exception": "Failed to open database"}}'
    )
    error(
        err=RuntimeError("Failed to open database"),
        msg="This is error message",
        alert_type="DBError",
    )
    captured = capsys.readouterr().out.split("\n")
    assert captured[0] == f"{LUMIGO_REPORT_ERROR_STRING} {msg}"


def test_err_with_no_type_and_no_exception(capsys):
    msg = '{"message": "This is error message", "type": "ProgrammaticError", "level": 40}'
    error(
        msg="This is error message",
    )
    captured = capsys.readouterr().out.split("\n")
    assert captured[0] == f"{LUMIGO_REPORT_ERROR_STRING} {msg}"


def test_basic_info_warn_error(capsys):
    info("This is error message")
    warn("This is error message")
    error("This is error message")
    info_msg = (
        '[LUMIGO_LOG] {"message": "This is error message", "type": "ProgrammaticInfo", "level": 20}'
    )
    warn_msg = (
        '[LUMIGO_LOG] {"message": "This is error message", "type": "ProgrammaticWarn", "level": 30}'
    )
    error_msg = '[LUMIGO_LOG] {"message": "This is error message", "type": "ProgrammaticError", "level": 40}'
    captured = capsys.readouterr().out.split("\n")
    assert captured[0] == info_msg
    assert captured[1] == warn_msg
    assert captured[2] == error_msg


def test_add_execution_tag():
    key = "my_key"
    value = "my_value"
    assert add_execution_tag(key, value) is True
    assert SpansContainer.get_span().function_span[EXECUTION_TAGS_KEY] == [
        {"key": key, "value": value}
    ]


def test_add_execution_key_tag_empty(capsys):
    assert add_execution_tag("", "value") is False
    assert "Unable to add tag: key length" in capsys.readouterr().out
    assert SpansContainer.get_span().function_span[EXECUTION_TAGS_KEY] == []


def test_add_execution_value_tag_empty(capsys):
    assert add_execution_tag("key", "") is False
    assert "Unable to add tag: value length" in capsys.readouterr().out
    assert SpansContainer.get_span().function_span[EXECUTION_TAGS_KEY] == []


def test_add_execution_tag_key_pass_max_chars(capsys):
    assert add_execution_tag("k" * (MAX_TAG_KEY_LEN + 1), "value") is False
    assert "Unable to add tag: key length" in capsys.readouterr().out
    assert SpansContainer.get_span().function_span[EXECUTION_TAGS_KEY] == []


def test_add_execution_tag_value_pass_max_chars(capsys):
    assert add_execution_tag("key", "v" * (MAX_TAG_VALUE_LEN + 1)) is False
    assert "Unable to add tag: value length" in capsys.readouterr().out
    assert SpansContainer.get_span().function_span[EXECUTION_TAGS_KEY] == []


def test_add_execution_tag_pass_max_tags(capsys):
    key = "my_key"
    value = "my_value"

    for i in range(MAX_TAGS + 1):
        result = add_execution_tag(key, value)
        if i < MAX_TAGS:
            assert result is True
        else:
            assert result is False

    assert "Unable to add tag: maximum number of tags" in capsys.readouterr().out
    assert (
        SpansContainer.get_span().function_span[EXECUTION_TAGS_KEY]
        == [{"key": key, "value": value}] * MAX_TAGS  # noqa
    )


def test_add_execution_tag_exception_catch(capsys):
    class ExceptionOnStr:
        def __str__(self):
            raise Exception()

    assert add_execution_tag("key", ExceptionOnStr()) is False
    assert "Unable to add tag" in capsys.readouterr().out
    assert SpansContainer.get_span().function_span[EXECUTION_TAGS_KEY] == []
