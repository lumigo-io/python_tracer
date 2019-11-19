import os
from importlib import import_module

from lumigo_tracer import lumigo_tracer

ORIGINAL_HANDLER_KEY = "LUMIGO_ORIGINAL_HANDLER"


@lumigo_tracer()
def _handler(*args, **kwargs):
    try:
        module_name, unit_name = os.environ[ORIGINAL_HANDLER_KEY].rsplit(".", 1)
        original_handler = getattr(import_module(module_name), unit_name)
    except (ImportError, AttributeError):
        raise ImportError(
            "Could not load the original handler. Are you sure that the import is ok?"
        )
    except KeyError:
        raise ValueError(
            "Could not find the original handler. Please follow lumigo's docs: https://docs.lumigo.io/"
        )
    except Exception:
        raise Exception(
            "Problem occurred in lumigo's wrapper. Make sure that you followed the docs or contact us."
        )
    return original_handler(*args, **kwargs)
