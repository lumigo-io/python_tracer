import importlib
import uuid
from typing import Optional

from lumigo_tracer.lambda_tracer.lambda_reporter import SQL_SPAN
from lumigo_tracer.lambda_tracer.spans_container import SpansContainer
from lumigo_tracer.libs.wrapt import wrap_function_wrapper
from lumigo_tracer.lumigo_utils import (
    get_current_ms_time,
    get_logger,
    lumigo_dumps,
    lumigo_safe_execute,
)

try:
    from sqlalchemy.event import listen
except Exception:
    listen = None


_last_span_id: Optional[str] = None


def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):  # type: ignore[no-untyped-def]
    global _last_span_id
    with lumigo_safe_execute("handle sqlalchemy before execute"):
        _last_span_id = str(uuid.uuid4())
        SpansContainer.get_span().add_span(
            {
                "id": _last_span_id,
                "type": SQL_SPAN,
                "started": get_current_ms_time(),
                "connectionParameters": {
                    "host": conn.engine.url.host or conn.engine.url.database,
                    "port": conn.engine.url.port,
                    "database": conn.engine.url.database,
                    "user": conn.engine.url.username,
                },
                "query": lumigo_dumps(statement),
                "values": lumigo_dumps(parameters),
            }
        )


def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):  # type: ignore[no-untyped-def]
    with lumigo_safe_execute("handle sqlalchemy after execute"):
        span = SpansContainer.get_span().get_span_by_id(_last_span_id)
        if not span:
            get_logger().warning("SQLAlchemy span ended without a record on its start")
            return
        span.update({"ended": get_current_ms_time(), "response": ""})


def _handle_error(context):  # type: ignore[no-untyped-def]
    with lumigo_safe_execute("handle sqlalchemy error"):
        span = SpansContainer.get_span().get_span_by_id(_last_span_id)
        if not span:
            get_logger().warning("SQLAlchemy span ended without a record on its start")
            return
        span.update(
            {
                "ended": get_current_ms_time(),
                "error": lumigo_dumps(
                    {
                        "type": context.original_exception.__class__.__name__,
                        "args": context.original_exception.args,
                    }
                ),
            }
        )


def execute_wrapper(func, instance, args, kwargs):  # type: ignore[no-untyped-def]
    result = func(*args, **kwargs)
    with lumigo_safe_execute("sqlalchemy: listen to engine"):
        listen(instance, "before_cursor_execute", _before_cursor_execute)
        listen(instance, "after_cursor_execute", _after_cursor_execute)
        listen(instance, "handle_error", _handle_error)
    return result


def wrap_sqlalchemy():  # type: ignore[no-untyped-def]
    with lumigo_safe_execute("wrap sqlalchemy"):
        if importlib.util.find_spec("sqlalchemy") and listen:
            get_logger().debug("wrapping sqlalchemy")
            wrap_function_wrapper("sqlalchemy.engine.base", "Engine.__init__", execute_wrapper)
