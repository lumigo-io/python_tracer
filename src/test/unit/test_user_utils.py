from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.user_utils import (
    report_error,
    LUMIGO_REPORT_ERROR_STRING,
    add_execution_tag,
    MAX_TAG_KEY_LEN,
    MAX_TAG_VALUE_LEN,
    MAX_TAGS,
)
from lumigo_tracer.utils import Configuration, EXECUTION_TAGS_KEY


def test_report_error_with_enhance_print(capsys):
    Configuration.enhanced_print = True
    msg = "oh no - an error"
    report_error(msg)
    captured = capsys.readouterr()
    assert captured.out == f"{LUMIGO_REPORT_ERROR_STRING} {msg}\n"


def test_report_error_without_enhance_print(capsys):
    Configuration.enhanced_print = False
    SpansContainer.get_span().function_span["id"] = "123"
    msg = "oh no - an error"
    report_error(msg)
    captured = capsys.readouterr()
    assert captured.out == f"RequestId: 123 {LUMIGO_REPORT_ERROR_STRING} {msg}\n"


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
