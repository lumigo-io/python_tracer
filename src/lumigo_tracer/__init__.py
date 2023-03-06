from lumigo_tracer.lambda_tracer.tracer import LumigoChalice, lumigo_tracer  # noqa

from .auto_instrument_handler import _handler  # noqa
from .lambda_tracer.global_scope_exec import global_scope_exec
from .user_utils import (  # noqa
    add_execution_tag,
    error,
    info,
    report_error,
    start_manual_trace,
    stop_manual_trace,
    warn,
)

global_scope_exec()
