from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.user_utils import (
    report_error,
    LUMIGO_REPORT_ERROR_STRING,
    add_execution_tag,
    MAX_TAG_KEY_LEN,
    MAX_TAG_VALUE_LEN,
    MAX_TAGS,
)
from lumigo_tracer.utils import Configuration


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
    add_execution_tag(key, value)
    assert SpansContainer.get_span().function_span["tags"] == [{"key": key, "value": value}]


def test_add_execution_key_tag_empty(capsys):
    add_execution_tag("", "value")
    assert "Unable to add tag: key length" in capsys.readouterr().out
    assert SpansContainer.get_span().function_span["tags"] == []


def test_add_execution_value_tag_empty(capsys):
    add_execution_tag("key", "")
    assert "Unable to add tag: value length" in capsys.readouterr().out
    assert SpansContainer.get_span().function_span["tags"] == []


def test_add_execution_tag_key_pass_max_chars(capsys):
    add_execution_tag("k" * (MAX_TAG_KEY_LEN + 1), "value")
    assert "Unable to add tag: key length" in capsys.readouterr().out
    assert SpansContainer.get_span().function_span["tags"] == []


def test_add_execution_tag_value_pass_max_chars(capsys):
    add_execution_tag("key", "v" * (MAX_TAG_VALUE_LEN + 1))
    assert "Unable to add tag: value length" in capsys.readouterr().out
    assert SpansContainer.get_span().function_span["tags"] == []


def test_add_execution_tag_pass_max_tags(capsys):
    key = "my_key"
    value = "my_value"

    for i in range(MAX_TAGS + 1):
        add_execution_tag(key, value)

    assert "Unable to add tag: maximum number of tags" in capsys.readouterr().out
    assert (
        SpansContainer.get_span().function_span["tags"] == [{"key": key, "value": value}] * MAX_TAGS
    )


def test_add_execution_tag_exception_catch(capsys):
    class ExceptionOnStr:
        def __str__(self):
            raise Exception()

    add_execution_tag("key", ExceptionOnStr())
    assert "Unable to add tag" in capsys.readouterr().out
    assert SpansContainer.get_span().function_span["tags"] == []
