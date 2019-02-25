import logging

from lumigo_tracer import utils
from lumigo_tracer.spans_container import SpansContainer
import mock
import pytest


@pytest.fixture(autouse=True)
def reporter_mock(monkeypatch):
    reporter_mock = mock.Mock(utils.report_json)
    monkeypatch.setattr(utils, "report_json", reporter_mock)
    return reporter_mock


@pytest.yield_fixture(autouse=True)
def restart_global_span():
    """
    This fixture initialize the span to be empty.
    """
    yield
    SpansContainer._span = None


@pytest.fixture(autouse=True)
def verbose_logger():
    """
    This fixture make sure that we will see all the log in the tests.
    """
    utils.get_logger().setLevel(logging.DEBUG)


def pytest_addoption(parser):
    parser.addoption("--all", action="store_true", default=False, help="run components tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--all"):
        return  # don't skip!
    skip_slow = pytest.mark.skip(reason="need --all option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
