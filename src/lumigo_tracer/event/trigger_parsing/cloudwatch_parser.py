from typing import Optional, Dict, Any

from lumigo_tracer.event.trigger_parsing.event_trigger_base import (
    EventTriggerParser,
    ExtraKeys,
    TriggerType,
)


class CloudwatchEventTriggerParser(EventTriggerParser):
    @staticmethod
    def _should_handle(event: Dict[Any, Any]) -> bool:
        return (
            event.get("detail-type") == "Scheduled Event" and "source" in event and "time" in event
        )

    @staticmethod
    def handle(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        return EventTriggerParser.build_trigger(
            target_id=target_id,
            resource_type="cloudwatch",
            extra={
                ExtraKeys.RESOURCE: event.get("resources", ["/unknown"])[0].split("/")[1],
                ExtraKeys.REGION: event.get("region"),
                ExtraKeys.DETAIL_TYPE: event.get("detail-type"),
            },
        )
