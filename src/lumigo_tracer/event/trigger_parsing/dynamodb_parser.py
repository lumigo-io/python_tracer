from typing import Optional, Dict, Any

from lumigo_tracer.event.trigger_parsing.event_trigger_base import (
    EventTriggerParser,
    ExtraKeys,
    TriggerType,
)
from lumigo_tracer.lumigo_utils import md5hash


class DynamoDBEventTriggerParser(EventTriggerParser):
    @staticmethod
    def _should_handle(event: Dict[Any, Any]) -> bool:
        return bool(event.get("Records", [{}])[0].get("eventSource") == "aws:dynamodb")

    @staticmethod
    def handle(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        creation_time = DynamoDBEventTriggerParser._get_ddb_approx_creation_time_ms(event)
        message_ids = []
        total_size_bytes: int = 0
        for record in event["Records"]:
            total_size_bytes += record["dynamodb"].get("SizeBytes", 0)
            event_name = record.get("eventName")
            if event_name in ("MODIFY", "REMOVE") and record.get("dynamodb", {}).get("Keys"):
                message_ids.append(md5hash(record["dynamodb"]["Keys"]))
            elif event_name == "INSERT" and record.get("dynamodb", {}).get("NewImage"):
                message_ids.append(md5hash(record["dynamodb"]["NewImage"]))

        return EventTriggerParser.build_trigger(
            target_id=target_id,
            resource_type="dynamodb",
            from_message_ids=message_ids,
            extra={
                ExtraKeys.ARN: event["Records"][0]["eventSourceARN"],
                ExtraKeys.RECORDS_NUM: len(event["Records"]),
                ExtraKeys.TRIGGER_CREATION_TIME: creation_time,
                ExtraKeys.TOTAL_SIZE: total_size_bytes,
            },
        )

    @staticmethod
    def _get_ddb_approx_creation_time_ms(event: Dict[Any, Any]) -> int:
        return (
            int(event["Records"][0].get("dynamodb", {}).get("ApproximateCreationDateTime", 0))
            * 1000
        )
