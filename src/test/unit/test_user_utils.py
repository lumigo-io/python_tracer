from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.user_utils import (
    report_error,
    LUMIGO_REPORT_ERROR_STRING,
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
