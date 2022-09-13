import os

try:
    # Try to import AWS's current _get_handler logic
    from bootstrap import _get_handler as aws_get_handler
except Exception:
    # Import a snapshot of _get_handler from python38 runtime
    from lumigo_tracer.libs.bootstrap import _get_handler as aws_get_handler

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


try:
    # import handler during runtime initialization, as usual.
    get_original_handler()
except Exception:
    pass
