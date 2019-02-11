from lumigo_tracer import reporter
import mock
import pytest


@pytest.fixture(autouse=True)
def reporter_mock(monkeypatch):
    reporter_mock = mock.Mock(reporter.report_json)
    monkeypatch.setattr(reporter, "report_json", reporter_mock)
    return reporter_mock
