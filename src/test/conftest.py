from lumigo_tracer import reporter
import mock
import pytest


@pytest.fixture(autouse=True)
def reporter_mock(monkeypatch):
    reporter_mock = mock.Mock(reporter.report_json)
    monkeypatch.setattr(reporter, "report_json", reporter_mock)
    return reporter_mock


def pytest_addoption(parser):
    parser.addoption("--all", action="store_true", default=False, help="run components tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--all"):
        return  # don't skip!
    skip_slow = pytest.mark.skip(reason="need --all option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
