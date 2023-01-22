from typing import List, Optional, Dict, Any

from lumigo_tracer.event.trigger_parsing.event_trigger_base import (
    EventTriggerParser,
    ExtraKeys,
    TriggerType,
)


class SqsEventTriggerParser(EventTriggerParser):
    @staticmethod
    def _should_handle(event: Dict[Any, Any]) -> bool:
        return bool(event.get("Records", [{}])[0].get("eventSource") == "aws:sqs") or bool(
            event.get("service_name") == "sqs" and event.get("operation_name") == "ReceiveMessage"
        )

    @staticmethod
    def _get_messages(event: Dict[Any, Any]) -> List[Dict[Any, Any]]:
        return event.get("Records", []) + event.get("Messages", [])  # type: ignore

    @staticmethod
    def handle(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        messages = SqsEventTriggerParser._get_messages(event)
        message_ids = []
        for record in messages:
            record_message_id = record.get("messageId") or record.get("MessageId")
            if not record_message_id:
                continue
            message_ids.append(record_message_id)

        arn = event.get("Records", [{}])[0].get("eventSourceARN") or "Unknown"
        return EventTriggerParser.build_trigger(
            target_id=target_id,
            resource_type="sqs",
            from_message_ids=message_ids,
            extra={
                ExtraKeys.ARN: arn,
                ExtraKeys.RECORDS_NUM: len(messages),
            },
        )

    @staticmethod
    def extract_inner(event: Dict[Any, Any]) -> List[str]:
        inner_messages = []
        for record in SqsEventTriggerParser._get_messages(event):
            body = record.get("body") or record.get("Body")
            if isinstance(body, str):
                inner_messages.append(body)
        return inner_messages
