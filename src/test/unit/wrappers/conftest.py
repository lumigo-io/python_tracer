import pytest

from lumigo_tracer.wrappers import wrap


@pytest.fixture(autouse=True)
def wrap_everything(aws_environment):
    wrap()
