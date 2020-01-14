import os
import importlib

from lumigo_tracer import lumigo_tracer

ORIGINAL_HANDLER_KEY = "LUMIGO_ORIGINAL_HANDLER"


@lumigo_tracer()
def _handler(*args, **kwargs):
    try:
        module_name, unit_name = os.environ[ORIGINAL_HANDLER_KEY].rsplit(".", 1)
        importable_name = module_name.replace("/", ".")
        original_handler = getattr(importlib.import_module(importable_name), unit_name)
    except (ImportError, AttributeError):
        raise ImportError(
            "Could not load the original handler. Are you sure that the import is ok?"
        )
    except KeyError:
        raise ValueError(
            "Could not find the original handler. Please follow lumigo's docs: https://docs.lumigo.io/"
        )
    except SyntaxError:
        raise SyntaxError("Syntax error in the original handler.")
    except Exception:
        raise Exception(
            "Problem occurred in lumigo's wrapper. Make sure that you followed the docs or contact us."
        )
    return original_handler(*args, **kwargs)
