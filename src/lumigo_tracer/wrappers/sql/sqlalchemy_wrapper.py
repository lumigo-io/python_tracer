import importlib

import time
import uuid

from lumigo_tracer.libs.wrapt import wrap_function_wrapper
from lumigo_tracer.lumigo_utils import lumigo_safe_execute, get_logger, lumigo_dumps
from lumigo_tracer.spans_container import SpansContainer

try:
    from sqlalchemy.event import listen
except Exception:
    listen = None


SQL_SPAN = "mySql"


def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    SpansContainer.get_span().add_span(
        {
            "id": str(uuid.uuid4()),
            "type": SQL_SPAN,
            "started": int(time.time() * 1000),
            "connectionParameters": {
                "host": conn.engine.url.host,
                "port": conn.engine.url.port,
                "database": conn.engine.url.database,
                "user": conn.engine.url.username,
            },
            "query": statement,
            "values": lumigo_dumps(parameters),
        }
    )


def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    span = SpansContainer.get_span().get_last_span()
    if not span:
        get_logger().warning("Redis span ended without a record on its start")
        return
    span.update({"ended": int(time.time() * 1000), "response": ""})


def _handle_error(context):
    span = SpansContainer.get_span().get_last_span()
    if not span:
        get_logger().warning("Redis span ended without a record on its start")
        return
    span.update(
        {
            "ended": int(time.time() * 1000),
            "error": lumigo_dumps(
                {
                    "type": context.original_exception.__class__.__name__,
                    "args": context.original_exception.args,
                }
            ),
        }
    )


def execute_wrapper(func, instance, args, kwargs):
    result = func(*args, **kwargs)
    with lumigo_safe_execute("sqlalchemy: listen to engine"):
        listen(result, "before_cursor_execute", _before_cursor_execute)
        listen(result, "after_cursor_execute", _after_cursor_execute)
        listen(result, "handle_error", _handle_error)
    return result


def wrap_sqlalchemy():
    with lumigo_safe_execute("wrap sqlalchemy"):
        if importlib.util.find_spec("sqlalchemy"):
            get_logger().debug("wrapping sqlalchemy")
            wrap_function_wrapper(
                "sqlalchemy.engine.strategies", "DefaultEngineStrategy.create", execute_wrapper
            )
