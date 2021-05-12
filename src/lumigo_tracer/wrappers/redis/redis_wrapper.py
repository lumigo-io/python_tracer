import copy
import importlib
from typing import Optional, Dict, List, Union
import uuid

from lumigo_tracer.libs.wrapt import wrap_function_wrapper
from lumigo_tracer.lumigo_utils import (
    lumigo_safe_execute,
    get_logger,
    lumigo_dumps,
    get_current_ms_time,
)
from lumigo_tracer.spans_container import SpansContainer


REDIS_SPAN = "redis"


def command_started(
    command: str, request_args: Union[Dict, List[Dict]], connection_options: Optional[Dict]
):
    span_id = str(uuid.uuid4())
    host = (connection_options or {}).get("host")
    port = (connection_options or {}).get("port")
    SpansContainer.get_span().add_span(
        {
            "id": span_id,
            "type": REDIS_SPAN,
            "started": get_current_ms_time(),
            "requestCommand": command,
            "requestArgs": lumigo_dumps(copy.deepcopy(request_args)),
            "connectionOptions": {"host": host, "port": port},
        }
    )


def command_finished(ret_val: Dict):
    with lumigo_safe_execute("redis command finished"):
        span = SpansContainer.get_span().get_last_span()
        if not span:
            get_logger().warning("Redis span ended without a record on its start")
            return
        span.update(
            {"ended": get_current_ms_time(), "response": lumigo_dumps(copy.deepcopy(ret_val))}
        )


def command_failed(exception: Exception):
    with lumigo_safe_execute("redis command failed"):
        span = SpansContainer.get_span().get_last_span()
        if not span:
            get_logger().warning("Redis span ended without a record on its start")
            return
        span.update(
            {"ended": get_current_ms_time(), "error": exception.args[0] if exception.args else None}
        )


def execute_command_wrapper(func, instance, args, kwargs):
    with lumigo_safe_execute("redis start"):
        command = args[0] if args else None
        request_args = args[1:] if args and len(args) > 1 else None
        connection_options = instance.connection_pool.connection_kwargs
        command_started(command, request_args, connection_options)
    try:
        ret_val = func(*args, **kwargs)
        command_finished(ret_val)
        return ret_val
    except Exception as e:
        command_failed(e)
        raise


def execute_wrapper(func, instance, args, kwargs):
    with lumigo_safe_execute("redis start"):
        commands = instance.command_stack
        command = [cmd[0] for cmd in commands if cmd] or None
        request_args = [cmd[1:] for cmd in commands if cmd and len(cmd) > 1]
        connection_options = instance.connection_pool.connection_kwargs
        command_started(lumigo_dumps(command), request_args, connection_options)
    try:
        ret_val = func(*args, **kwargs)
        command_finished(ret_val)
        return ret_val
    except Exception as e:
        command_failed(e)
        raise


def wrap_redis():
    with lumigo_safe_execute("wrap redis"):
        if importlib.util.find_spec("redis"):
            get_logger().debug("wrapping redis")
            wrap_function_wrapper("redis.client", "Redis.execute_command", execute_command_wrapper)
            wrap_function_wrapper("redis.client", "Pipeline.execute", execute_wrapper)
