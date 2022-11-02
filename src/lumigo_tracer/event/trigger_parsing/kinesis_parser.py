from typing import Optional, Dict, Any

from lumigo_tracer.event.trigger_parsing.event_trigger_base import (
    EventTriggerParser,
    ExtraKeys,
    TriggerType,
)
from lumigo_tracer.parsing_utils import safe_get


class KinesisEventTriggerParser(EventTriggerParser):
    @staticmethod
    def _should_handle(event: Dict[Any, Any]) -> bool:
        return bool(event.get("Records", [{}])[0].get("eventSource") == "aws:kinesis")

    @staticmethod
    def handle(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        extra = {}
        message_ids = []
        records = safe_get(event, ["Records"], default=[])
        for record in records:
            message_id = safe_get(record, ["kinesis", "sequenceNumber"])
            if message_id:
                message_ids.append(message_id)

        event_id = safe_get(event, ["Records", 0, "eventID"])
        if isinstance(event_id, str):
            extra[ExtraKeys.SHARD_ID] = event_id.split(":", 1)[0]

        return EventTriggerParser.build_trigger(
            target_id=target_id,
            resource_type="kinesis",
            from_message_ids=message_ids,
            extra={
                ExtraKeys.ARN: event["Records"][0]["eventSourceARN"],
                ExtraKeys.RECORDS_NUM: len(event["Records"]),
                **extra,
            },
        )
