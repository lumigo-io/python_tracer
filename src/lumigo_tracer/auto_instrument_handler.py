import warnings
import os
import importlib

from lumigo_tracer import lumigo_tracer

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    import imp

ORIGINAL_HANDLER_KEY = "LUMIGO_ORIGINAL_HANDLER"


def parse_handler():
    try:
        module_name, unit_name = os.environ[ORIGINAL_HANDLER_KEY].rsplit(".", 1)
        file_handle, pathname, desc = imp.find_module(module_name)
    except KeyError:
        raise Exception(
            "Could not find the original handler. Please contact Lumigo for more information."
        ) from None
    except ValueError as e:
        raise ValueError(
            f"Runtime.MalformedHandlerName: Bad handler '{os.environ[ORIGINAL_HANDLER_KEY]}': {str(e)}"
        ) from None
    return module_name, unit_name, file_handle, pathname, desc


@lumigo_tracer()
def _handler(*args, **kwargs):
    handler_module = ""
    try:
        handler_module, unit_name, file_handle, pathname, desc = parse_handler()
        original_module = imp.load_module(handler_module, file_handle, pathname, desc)
    except ImportError as e:
        raise ImportError(
            f"Runtime.ImportModuleError: Unable to import module '{handler_module}': {str(e)}"
        ) from None
    except SyntaxError as e:
        raise SyntaxError(
            f"Runtime.UserCodeSyntaxError: Syntax error in module '{handler_module}': {str(e)}"
        ) from None
    try:
        original_handler = getattr(original_module, unit_name)
    except AttributeError:
        raise Exception(
            f"Runtime.HandlerNotFound: Handler '{unit_name}' missing on module '{handler_module}'"
        ) from None
    return original_handler(*args, **kwargs)


try:
    # import handler during runtime initialization, as usual.
    importlib.import_module(parse_handler()[0])
except Exception:
    pass
