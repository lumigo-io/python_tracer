import importlib
import sys

import lumigo_tracer


def test_wrapping_without_libraries(monkeypatch):
    # remove pymongo from path and reload module
    monkeypatch.setitem(sys.modules, "pymongo", None)
    wrapper = importlib.reload(lumigo_tracer.wrappers.pymongo.pymongo_wrapper)
    assert wrapper.LumigoMongoMonitoring is None

    lumigo_tracer.wrappers.wrap()  # should succeed
