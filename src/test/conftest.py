import base64
import builtins
import gzip
import logging
import os
import shutil
from types import SimpleNamespace

import mock
import pytest
from lumigo_core.logger import get_logger
from lumigo_core.scrubbing import get_omitting_regex

from lumigo_tracer import lumigo_utils, wrappers
from lumigo_tracer.lambda_tracer import lambda_reporter
from lumigo_tracer.lambda_tracer.lambda_reporter import get_edge_host
from lumigo_tracer.lambda_tracer.spans_container import SpansContainer
from lumigo_tracer.lumigo_utils import Configuration, InternalState
from lumigo_tracer.wrappers.http.http_data_classes import HttpState

USE_TRACER_EXTENSION = "LUMIGO_USE_TRACER_EXTENSION"


@pytest.fixture(autouse=True)
def reporter_mock(monkeypatch, request):
    if request.node.get_closest_marker("dont_mock_lumigo_utils_reporter"):
        return
    lumigo_utils.Configuration.should_report = False
    reporter_mock = mock.Mock(lambda_reporter.report_json)
    reporter_mock.return_value = 123
    monkeypatch.setattr(lambda_reporter, "report_json", reporter_mock)
    return reporter_mock


@pytest.fixture(autouse=True)
def cancel_timeout_mechanism(monkeypatch):
    monkeypatch.setattr(Configuration, "timeout_timer", False)


@pytest.fixture()
def with_extension(monkeypatch):
    monkeypatch.setenv(USE_TRACER_EXTENSION, "TRUE")


@pytest.fixture(autouse=True)
def remove_caches(monkeypatch):
    get_omitting_regex.cache_clear()
    get_edge_host.cache_clear()
    monkeypatch.setattr(lambda_reporter, "edge_kinesis_boto_client", None)


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


@pytest.fixture
def aws_environment(monkeypatch):
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_VERSION", "true")


@pytest.fixture(autouse=True)
def extension_clean():
    yield
    if os.path.exists("/tmp/lumigo-spans"):
        shutil.rmtree("/tmp/lumigo-spans")


@pytest.fixture
def token():
    return "t_10faa5e13e7844aaa1234"


@pytest.fixture
def aws_env(monkeypatch):
    monkeypatch.setenv(
        "_X_AMZN_TRACE_ID",
        "Root=1-12345678-111111111111111111111111;Parent=blablablablabla;Sampled=0",
    )


@pytest.fixture
def lambda_traced(monkeypatch, aws_environment):
    monkeypatch.setenv("LUMIGO_SWITCH_OFF", "false")


@pytest.fixture
def wrap_all_libraries(lambda_traced):
    wrappers.wrap()


@pytest.fixture
def unzip_zipped_spans():
    """
    Pytest fixture that provides a function to unzip and decode zipped spans.
    """

    def _unzip(zipped_spans: str) -> str:
        # Step 1: Decode the base64 encoded string back to bytes
        compressed_data = base64.b64decode(zipped_spans)

        # Step 2: Decompress the gzip data
        decompressed_data = gzip.decompress(compressed_data)

        # Step 3: Decode the bytes back to a string
        return decompressed_data.decode("utf-8")

    return _unzip
