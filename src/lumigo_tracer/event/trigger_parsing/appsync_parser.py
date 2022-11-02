from typing import Optional, Dict, Any

from lumigo_tracer.event.trigger_parsing.event_trigger_base import (
    EventTriggerParser,
    ExtraKeys,
    TriggerType,
)
from lumigo_tracer.parsing_utils import safe_get, safe_split_get


class AppsyncEventTriggerParser(EventTriggerParser):
    @staticmethod
    def _should_handle(event: Dict[Any, Any]) -> bool:
        host = safe_get(event, ["context", "request", "headers", "host"])
        if not host:
            host = safe_get(event, ["request", "headers", "host"])
        return isinstance(host, str) and "appsync-api" in host

    @staticmethod
    def handle(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        headers = safe_get(event, ["context", "request", "headers"], default={})
        if not headers:
            headers = safe_get(event, ["request", "headers"])
        trace_id = headers.get("x-amzn-trace-id")
        message_id = safe_split_get(trace_id, "=", -1)
        return EventTriggerParser.build_trigger(
            target_id=target_id,
            resource_type="appsync",
            from_message_ids=[message_id],
            extra={ExtraKeys.API: headers.get("host")},
        )
