"""
Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""

import importlib
import sys

from .lambda_runtime_exception import FaultException


def _get_handler(handler):
    try:
        (modname, fname) = handler.rsplit(".", 1)
    except ValueError as e:
        fault = FaultException(
            FaultException.MALFORMED_HANDLER_NAME,
            "Bad handler '{}': {}".format(handler, str(e)),
        )
        return make_fault_handler(fault)

    try:
        if modname.split(".")[0] in sys.builtin_module_names:
            fault = FaultException(
                FaultException.BUILT_IN_MODULE_CONFLICT,
                "Cannot use built-in module {} as a handler module".format(modname),
            )
            return make_fault_handler(fault)
        m = importlib.import_module(modname.replace("/", "."))
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
