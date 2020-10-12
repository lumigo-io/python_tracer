import importlib
import sys

import lumigo_tracer


def test_wrapping_without_libraries(monkeypatch):
    # remove library from path and reload module
    monkeypatch.setitem(sys.modules, "pymongo", None)
    wrapper = importlib.reload(lumigo_tracer.wrappers.pymongo.pymongo_wrapper)
    assert wrapper.LumigoMongoMonitoring is None

    monkeypatch.setitem(sys.modules, "redis", None)
    importlib.reload(lumigo_tracer.wrappers.redis.redis_wrapper)

    lumigo_tracer.wrappers.wrap()  # should succeed

    monkeypatch.undo()
    importlib.reload(lumigo_tracer.wrappers.pymongo.pymongo_wrapper)
    importlib.reload(lumigo_tracer.wrappers.redis.redis_wrapper)
