from typing import Optional, Dict, Any

from lumigo_tracer.event.trigger_parsing.event_trigger_base import EventTriggerParser, TriggerType
from lumigo_tracer.lumigo_utils import Configuration, LUMIGO_EVENT_KEY, STEP_FUNCTION_UID_KEY
from lumigo_tracer.parsing_utils import recursive_get_key


class StepFunctionEventTriggerParser(EventTriggerParser):
    @staticmethod
    def _should_handle(event: Dict[Any, Any]) -> bool:
        return (
            Configuration.is_step_function
            and isinstance(event, (list, dict))
            and STEP_FUNCTION_UID_KEY in recursive_get_key(event, LUMIGO_EVENT_KEY, default={})
        )

    @staticmethod
    def handle(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        return EventTriggerParser.build_trigger(
            target_id=target_id,
            resource_type="stepFunction",
            from_message_ids=[recursive_get_key(event, LUMIGO_EVENT_KEY)[STEP_FUNCTION_UID_KEY]],
        )
