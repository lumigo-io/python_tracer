import os
import sys

from lumigo_tracer.lumigo_utils import is_aws_environment

try:
    # Try to import AWS's current _get_handler logic
    from bootstrap import _get_handler as aws_get_handler
except Exception:
    # Import a snapshot of _get_handler from the lambda runtime interface client (awslambdaric)
    # matching the current python version
    if sys.version_info >= (3, 7):
        # This version supports python 3.7 up to 3.12
        from lumigo_tracer.libs.awslambdaric.release_2_0_8.bootstrap import (
            _get_handler as aws_get_handler,
        )
    else:
        # This is the latest version that supports python 3.6, so we default to it
        # (We don't support python 3.5 or below)
        from lumigo_tracer.libs.awslambdaric.release_2_0_0.bootstrap import (
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
