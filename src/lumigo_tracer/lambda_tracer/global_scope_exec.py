from lumigo_tracer import auto_instrument_handler, wrappers
from lumigo_tracer.lambda_tracer import lambda_reporter
from lumigo_tracer.lumigo_utils import is_aws_environment


def global_scope_exec() -> None:
    if is_aws_environment():
        # Connection to edge: build the session
        lambda_reporter.establish_connection_global()
        # auto_instrument: import handler during runtime initialization, as usual.
        auto_instrument_handler.prefetch_handler_import()
        # follow requests to third party services
        wrappers.wrap()
