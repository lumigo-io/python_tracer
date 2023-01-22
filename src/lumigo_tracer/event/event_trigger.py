import json
from typing import List, Optional, Dict, Any

from lumigo_tracer.event.trigger_parsing import EVENT_TRIGGER_PARSERS, INNER_MESSAGES_MAGIC_PATTERN
from lumigo_tracer.event.trigger_parsing.event_trigger_base import TriggerType
from lumigo_tracer.lumigo_utils import Configuration, get_logger


def recursive_parse_trigger(
    message: Dict[Any, Any], parent_id: Optional[str] = None, level: int = 0
) -> List[TriggerType]:
    triggers = []
    if level >= Configuration.chained_services_max_depth:
        get_logger().info("Chained services parsing has stopped due to depth")
        return []
    for parser in EVENT_TRIGGER_PARSERS:
        if parser.should_handle(message):
            new_trigger = parser.handle(event=message, target_id=parent_id)
            triggers.append(new_trigger)
            current_trigger_id: str = new_trigger["id"]  # type: ignore

            inner_messages = parser.extract_inner(event=message)
            if len(inner_messages) >= Configuration.chained_services_max_width:
                get_logger().info("Chained services parsing has stopped due to width")
                inner_messages = inner_messages[: Configuration.chained_services_max_width]
            for sub_message in inner_messages:
                if INNER_MESSAGES_MAGIC_PATTERN.search(sub_message):
                    # We want to load only relevant messages, so first run a quick scan
                    triggers.extend(
                        recursive_parse_trigger(
                            json.loads(sub_message), parent_id=current_trigger_id, level=level + 1
                        )
                    )
            break
    return triggers


def parse_triggers(event: Dict[Any, Any]) -> List[Dict[Any, Any]]:
    return recursive_parse_trigger(event, parent_id=None, level=0)
