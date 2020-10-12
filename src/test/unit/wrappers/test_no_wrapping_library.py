import importlib

import sys


def test_wrapping_without_libraries(monkeypatch):
    monkeypatch.setitem(sys.modules, "pymongo", None)

    wrapper = importlib.import_module("src.lumigo_tracer.wrappers")
    wrapper.wrap()  # should succeed

    assert wrapper.pymongo.pymongo_wrapper.LumigoMongoMonitoring is None
