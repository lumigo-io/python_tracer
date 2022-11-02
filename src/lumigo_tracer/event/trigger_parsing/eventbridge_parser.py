from typing import List, Optional, Dict, Any

from lumigo_tracer.event.trigger_parsing.event_trigger_base import EventTriggerParser, TriggerType


class EventbridgeEventTriggerParser(EventTriggerParser):
    @staticmethod
    def _should_handle(event: Dict[Any, Any]) -> bool:
        return (
            isinstance(event.get("version"), str)
            and isinstance(event.get("id"), str)
            and isinstance(event.get("detail-type"), str)
            and isinstance(event.get("source"), str)
            and isinstance(event.get("time"), str)
            and isinstance(event.get("region"), str)
            and isinstance(event.get("resources"), list)
            and isinstance(event.get("detail"), dict)
        )

    @staticmethod
    def handle(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        return EventTriggerParser.build_trigger(
            target_id=target_id,
            resource_type="eventBridge",
            from_message_ids=[event["id"]],
        )

    @staticmethod
    def extract_inner(event: Dict[Any, Any]) -> List[Dict[Any, Any]]:
        # TODO
        return []
