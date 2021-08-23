"""
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""

import warnings

from lumigo_tracer.libs.lambda_runtime_exception import FaultException


with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    import imp

ERROR_LOG_LINE_TERMINATE = "\r"
ERROR_LOG_IDENT = "\u00a0"  # NO-BREAK SPACE U+00A0


def _get_handler(handler):
    try:
        (modname, fname) = handler.rsplit(".", 1)
    except ValueError as e:
        fault = FaultException(
            FaultException.MALFORMED_HANDLER_NAME, "Bad handler '{}': {}".format(handler, str(e))
        )
        return make_fault_handler(fault)

    file_handle, pathname, desc = None, None, None
    try:
        # Recursively loading handler in nested directories
        for segment in modname.split("."):
            if pathname is not None:
                pathname = [pathname]
            file_handle, pathname, desc = imp.find_module(segment, pathname)
        if file_handle is None:
            module_type = desc[2]
            if module_type == imp.C_BUILTIN:
                fault = FaultException(
                    FaultException.BUILT_IN_MODULE_CONFLICT,
                    "Cannot use built-in module {} as a handler module".format(modname),
                )
                request_handler = make_fault_handler(fault)
                return request_handler
        m = imp.load_module(modname, file_handle, pathname, desc)
    except ImportError as e:
        fault = FaultException(
            FaultException.IMPORT_MODULE_ERROR,
            "Unable to import module '{}': {}".format(modname, str(e)),
        )
        request_handler = make_fault_handler(fault)
        return request_handler
    except SyntaxError as e:
        trace = ['  File "%s" Line %s\n    %s' % (e.filename, e.lineno, e.text)]
        fault = FaultException(
            FaultException.USER_CODE_SYNTAX_ERROR,
            "Syntax error in module '{}': {}".format(modname, str(e)),
            trace,
        )
        request_handler = make_fault_handler(fault)
        return request_handler
    finally:
        if file_handle is not None:
            file_handle.close()

    try:
        request_handler = getattr(m, fname)
    except AttributeError:
        fault = FaultException(
            FaultException.HANDLER_NOT_FOUND,
            "Handler '{}' missing on module '{}'".format(fname, modname),
            None,
        )
        request_handler = make_fault_handler(fault)
    return request_handler


def make_fault_handler(fault):
    def result(*args):
        raise fault

    return result
