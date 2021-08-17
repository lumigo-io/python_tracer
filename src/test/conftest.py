import builtins
import logging
import os
import shutil
from types import SimpleNamespace

import mock
import pytest

from lumigo_tracer import lumigo_utils
from lumigo_tracer.lumigo_utils import (
    Configuration,
    get_omitting_regex,
    get_logger,
    get_edge_host,
    InternalState,
)
from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.wrappers.http.http_data_classes import HttpState


@pytest.fixture(autouse=True)
def reporter_mock(monkeypatch):
    lumigo_utils.Configuration.should_report = False
    reporter_mock = mock.Mock(lumigo_utils.report_json)
    reporter_mock.return_value = 123
    monkeypatch.setattr(lumigo_utils, "report_json", reporter_mock)
    return reporter_mock


@pytest.fixture(autouse=True)
def cancel_timeout_mechanism(monkeypatch):
    monkeypatch.setattr(Configuration, "timeout_timer", False)


@pytest.fixture(autouse=True)
def remove_caches(monkeypatch):
    get_omitting_regex.cache_clear()
    get_edge_host.cache_clear()
    monkeypatch.setattr(lumigo_utils, "edge_kinesis_boto_client", None)


@pytest.yield_fixture(autouse=True)
def restart_global_span():
    """
    This fixture initialize the span to be empty.
    """
    yield
    SpansContainer._span = None
    HttpState.clear()
    InternalState.reset()


@pytest.yield_fixture(autouse=True)
def reset_print():
    """
    Resets print
    """
    local_print = print
    yield
    builtins.print = local_print


@pytest.fixture(autouse=True)
def verbose_logger():
    """
    This fixture make sure that we will see all the log in the tests.
    """
    lumigo_utils.get_logger().setLevel(logging.DEBUG)
    lumigo_utils.config(should_report=False, verbose=True)


def pytest_addoption(parser):
    parser.addoption("--all", action="store_true", default=False, help="run components tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--all"):
        return  # don't skip!
    skip_slow = pytest.mark.skip(reason="need --all option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture(autouse=True)
def capture_all_logs(caplog):
    caplog.set_level(logging.DEBUG, logger="lumigo")
    get_logger().propagate = True


@pytest.fixture
def context():
    return SimpleNamespace(aws_request_id="1234", get_remaining_time_in_millis=lambda: 1000 * 2)


@pytest.fixture(autouse=True)
def extension_clean():
    yield
    if os.path.exists("/tmp/lumigo-spans"):
        shutil.rmtree("/tmp/lumigo-spans")
