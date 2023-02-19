from lumigo_core.triggers.trigger_parsing import EVENT_TRIGGER_PARSERS

from lumigo_tracer.event.trigger_parsing.step_function_parser import (
    StepFunctionEventTriggerParser,
)

EVENT_TRIGGER_PARSERS.append(StepFunctionEventTriggerParser)
