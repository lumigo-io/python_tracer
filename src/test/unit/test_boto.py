import sys
from importlib import reload
from unittest import mock

import pytest
from lumigo_tracer import boto


@pytest.fixture(autouse=True)
def reload_boto():
    yield
    reload(boto)


def test_get_boto_client():
    client = boto.get_boto_client("lambda", "eu-west-1", "key1", "secret1")
    assert hasattr(client, "list_functions")


def test_get_boto_client_boto_not_exist():
    with mock.patch.dict(sys.modules):
        sys.modules["boto3"] = None
        reload(boto)
        client = boto.get_boto_client("lambda", "eu-west-1", "key1", "secret1")
        assert client is None
