from typing import Optional, Dict, Any

from lumigo_tracer.event.trigger_parsing.event_trigger_base import (
    EventTriggerParser,
    ExtraKeys,
    TriggerType,
)
from lumigo_tracer.parsing_utils import safe_get


class SnsEventTriggerParser(EventTriggerParser):
    MAGIC_IDENTIFIER = r"SimpleNotificationService"

    @staticmethod
    def _should_handle(event: Dict[Any, Any]) -> bool:
        return bool(
            event.get("Records", [{}])[0].get("EventSource") == "aws:sns"
            or (event.get("Type") == "Notification" and "TopicArn" in event)
        )

    @staticmethod
    def handle(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        sns_record = safe_get(event, ["Records", 0, "Sns"]) or event
        return EventTriggerParser.build_trigger(
            target_id=target_id,
            resource_type="sns",
            from_message_ids=[sns_record["MessageId"]],
            extra={
                ExtraKeys.ARN: sns_record["TopicArn"],
                ExtraKeys.RECORDS_NUM: len(event.get("Records", [{}])),
            },
        )
