import inspect
from functools import wraps

from lumigo_tracer.auto_tag.auto_tag_event import AutoTagEvent
from lumigo_tracer.lumigo_utils import (
    config,
    get_logger,
    lumigo_safe_execute,
    is_aws_environment,
    is_kill_switch_on,
)
from lumigo_tracer.spans_container import SpansContainer, TimeoutMechanism
from lumigo_tracer.wrappers import wrap

CONTEXT_WRAPPED_BY_LUMIGO_KEY = "_wrapped_by_lumigo"


def _is_context_already_wrapped(*args) -> bool:
    """
    This function is here in order to validate that we didn't already wrap this lambda
        (using the sls plugin / auto instrumentation / etc.)
    """
    return len(args) >= 2 and hasattr(args[1], CONTEXT_WRAPPED_BY_LUMIGO_KEY)


def _add_wrap_flag_to_context(*args):
    """
    This function is here in order to validate that we didn't already wrap this invocation
        (using the sls plugin / auto instrumentation / etc.).
    We are adding lumigo's flag to the context, and check it's value in _is_context_already_wrapped.
    """
    if len(args) >= 2:
        with lumigo_safe_execute("wrap context"):
            setattr(args[1], CONTEXT_WRAPPED_BY_LUMIGO_KEY, True)


def _lumigo_tracer(func):
    if is_kill_switch_on():
        return func
    wrap()

    @wraps(func)
    def lambda_wrapper(*args, **kwargs):
        if _is_context_already_wrapped(*args):
            return func(*args, **kwargs)
        _add_wrap_flag_to_context(*args)
        executed = False
        ret_val = None
        try:
            SpansContainer.create_span(*args, is_new_invocation=True)
            with lumigo_safe_execute("auto tag"):
                AutoTagEvent.auto_tag_event(args[0])
            SpansContainer.get_span().start(*args)
            try:
                executed = True
                ret_val = func(*args, **kwargs)
            except Exception as e:
                with lumigo_safe_execute("Customer's exception"):
                    SpansContainer.get_span().add_exception_event(e, inspect.trace())
                raise
            finally:
                with lumigo_safe_execute("end"):
                    SpansContainer.get_span().end(ret_val, *args)
            return ret_val
        except Exception:
            # The case where our wrapping raised an exception
            if not executed:
                TimeoutMechanism.stop()
                get_logger().exception("exception in the wrapper", exc_info=True)
                return func(*args, **kwargs)
            else:
                raise

    return lambda_wrapper


def _add_prefix_for_each_line(prefix: str, text: str):
    enhanced_lines = []
    for line in text.split("\n"):
        if line and not line.startswith(prefix):
            line = prefix + " " + line
        enhanced_lines.append(line)
    return "\n".join(enhanced_lines)


def lumigo_tracer(*args, **kwargs):
    """
    This function should be used as wrapper to your lambda function.
    It will trace your HTTP calls and send it to our backend, which will help you understand it better.

    If the kill switch is activated (env variable `LUMIGO_SWITCH_OFF` set to 1), this function does nothing.

    You can pass to this decorator more configurations to configure the interface to lumigo,
        See `lumigo_tracer.reporter.config` for more details on the available configuration.
    """
    config(*args, **kwargs)
    return _lumigo_tracer


class LumigoChalice:
    DECORATORS_OF_NEW_HANDLERS = [
        "on_s3_event",
        "on_sns_message",
        "on_sqs_message",
        "schedule",
        "authorizer",
        "lambda_function",
        "on_cw_event",
        "on_dynamodb_record",
    ]

    def __init__(self, app, *args, **kwargs):
        self.lumigo_conf_args = args
        self.lumigo_conf_kwargs = kwargs
        self.app = app
        self.original_app_attr_getter = app.__getattribute__
        self.lumigo_app = lumigo_tracer(*self.lumigo_conf_args, **self.lumigo_conf_kwargs)(app)

    def __getattr__(self, item):
        original_attr = self.original_app_attr_getter(item)
        if is_aws_environment() and item in self.DECORATORS_OF_NEW_HANDLERS:

            def get_decorator(*args, **kwargs):
                # calling the annotation, example `app.authorizer(THIS)`
                chalice_actual_decorator = original_attr(*args, **kwargs)

                def wrapper2(func):
                    user_func_wrapped_by_chalice = chalice_actual_decorator(func)
                    return LumigoChalice(
                        user_func_wrapped_by_chalice,
                        *self.lumigo_conf_args,
                        **self.lumigo_conf_kwargs,
                    )

                return wrapper2

            return get_decorator
        return original_attr

    def __call__(self, *args, **kwargs):
        if len(args) < 2 and "context" not in kwargs:
            kwargs["context"] = getattr(getattr(self.app, "current_request", None), "context", None)
        return self.lumigo_app(*args, **kwargs)
