from typing import List, Optional, Dict, Any

from lumigo_tracer.event.trigger_parsing.event_trigger_base import (
    EventTriggerParser,
    ExtraKeys,
    TriggerType,
)


class SqsEventTriggerParser(EventTriggerParser):
    @staticmethod
    def _should_handle(event: Dict[Any, Any]) -> bool:
        return bool(event.get("Records", [{}])[0].get("eventSource") == "aws:sqs")

    @staticmethod
    def handle(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        message_ids = []
        for record in event.get("Records", []):
            record_message_id = record.get("messageId")
            if not record_message_id:
                continue
            message_ids.append(record_message_id)

        return EventTriggerParser.build_trigger(
            target_id=target_id,
            resource_type="sqs",
            from_message_ids=message_ids,
            extra={
                ExtraKeys.ARN: event["Records"][0]["eventSourceARN"],
                ExtraKeys.RECORDS_NUM: len(event["Records"]),
            },
        )

    @staticmethod
    def extract_inner(event: Dict[Any, Any]) -> List[str]:
        inner_messages = []
        for record in event.get("Records", []):
            body = record.get("body")
            if isinstance(body, str):
                inner_messages.append(body)
        return inner_messages
