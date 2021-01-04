import os
import importlib

from lumigo_tracer import lumigo_tracer

ORIGINAL_HANDLER_KEY = "LUMIGO_ORIGINAL_HANDLER"


def parse_handler():
    try:
        module_name, unit_name = os.environ[ORIGINAL_HANDLER_KEY].rsplit(".", 1)
    except KeyError:
        raise Exception(
            "Could not find the original handler. Please contact Lumigo for more information."
        ) from None
    except ValueError as e:
        raise ValueError(
            f"Runtime.MalformedHandlerName: Bad handler '{os.environ[ORIGINAL_HANDLER_KEY]}': {str(e)}"
        ) from None
    importable_name = module_name.replace("/", ".")
    return importable_name, unit_name


@lumigo_tracer()
def _handler(*args, **kwargs):
    handler_module = ""
    try:
        handler_module, unit_name = parse_handler()
        original_module = importlib.import_module(handler_module)
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
