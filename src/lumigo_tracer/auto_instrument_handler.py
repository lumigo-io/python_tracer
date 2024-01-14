import os

from lumigo_tracer.lumigo_utils import is_aws_environment

try:
    # Try importing get handler function from aws runtime interface client (awslambdaric) - should be available in
    # standard python lambda runtimes
    from awslambdaric.bootstrap import _get_handler as aws_get_handler
except Exception:
    try:
        # Before python 3.8 runtime the get handler function was available in the bootstrap module directly
        from bootstrap import _get_handler as aws_get_handler
    except Exception:

        # Only import the logger if we need it, no need to slow down the import time
        from lumigo_tracer.lumigo_utils import warn_client

        warn_client(
            "Could not import built in AWS awslambdaric.bootstrap._get_handler or bootstrap._get_handler function "
            "(Should be importable in standard aws lambda python runtime). Using fallback, "
            "Please contact Lumigo for more information."
        )

        # Import a snapshot of _get_handler from the lambda runtime interface client (awslambdaric),
        # in the unlikely case that the bootstrap module is not available
        from lumigo_tracer.libs.awslambdaric.bootstrap import (
            _get_handler as aws_get_handler,
        )

from lumigo_tracer import lumigo_tracer

ORIGINAL_HANDLER_KEY = "LUMIGO_ORIGINAL_HANDLER"


def get_original_handler():  # type: ignore[no-untyped-def]
    try:
        return aws_get_handler(os.environ[ORIGINAL_HANDLER_KEY])
    except KeyError:
        raise Exception(
            "Could not find the original handler. Please contact Lumigo for more information."
        ) from None


@lumigo_tracer()
def _handler(*args, **kwargs):  # type: ignore[no-untyped-def]
    original_handler = get_original_handler()
    return original_handler(*args, **kwargs)


def prefetch_handler_import() -> None:
    """
    This function imports the handler.
    When we call it in the global scope, it will be executed during the lambda initialization,
        thus will mimic the usual behavior.
    """
    if not is_aws_environment():
        return
    try:
        get_original_handler()
    except Exception:
        pass
