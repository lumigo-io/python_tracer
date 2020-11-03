import os
import importlib

from lumigo_tracer import lumigo_tracer

ORIGINAL_HANDLER_KEY = "LUMIGO_ORIGINAL_HANDLER"


def parse_handler():
    try:
        module_name, unit_name = os.environ[ORIGINAL_HANDLER_KEY].rsplit(".", 1)
    except KeyError:
        raise ValueError(
            "Could not find the original handler. Please contact Lumigo for more information."
        )
    except ValueError:
        raise RuntimeError(
            f"Invalid handler format: Bad handler '{os.environ[ORIGINAL_HANDLER_KEY]}': not enough values to unpack (expected 2, got 1)"
        )
    importable_name = module_name.replace("/", ".")
    return importable_name, unit_name


@lumigo_tracer()
def _handler(*args, **kwargs):
    handler_module = ""
    try:
        handler_module, unit_name = parse_handler()
        original_handler = getattr(importlib.import_module(handler_module), unit_name)
    except (ImportError, AttributeError):
        raise ImportError(
            f"Unable to import module '{handler_module}': No module named '{handler_module}'"
        )
    return original_handler(*args, **kwargs)


try:
    # import handler during runtime initialization, as usual.
    importlib.import_module(parse_handler()[0])
except Exception:
    pass
