from lumigo_tracer import reporter
from lumigo_tracer.span import Span
import mock
import pytest


@pytest.fixture(autouse=True)
def reporter_mock(monkeypatch):
    reporter_mock = mock.Mock(reporter.report_json)
    monkeypatch.setattr(reporter, "report_json", reporter_mock)
    return reporter_mock


@pytest.yield_fixture(autouse=True)
def restart_global_span():
    """
    This fixture initialize the span to be empty.
    """
    yield
    Span._span = None


def pytest_addoption(parser):
    parser.addoption("--all", action="store_true", default=False, help="run components tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--all"):
        return  # don't skip!
    skip_slow = pytest.mark.skip(reason="need --all option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
