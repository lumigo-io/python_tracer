from typing import Optional, Dict, Any

from lumigo_tracer.event.trigger_parsing.event_trigger_base import (
    EventTriggerParser,
    ExtraKeys,
    TriggerType,
)


class S3EventTriggerParser(EventTriggerParser):
    @staticmethod
    def _should_handle(event: Dict[Any, Any]) -> bool:
        return bool(event.get("Records", [{}])[0].get("eventSource") == "aws:s3")

    @staticmethod
    def handle(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        return EventTriggerParser.build_trigger(
            target_id=target_id,
            resource_type="s3",
            from_message_ids=[
                event["Records"][0].get("responseElements", {}).get("x-amz-request-id")
            ],
            extra={
                ExtraKeys.ARN: event["Records"][0]["s3"]["bucket"]["arn"],
                ExtraKeys.RECORDS_NUM: len(event["Records"]),
            },
        )
