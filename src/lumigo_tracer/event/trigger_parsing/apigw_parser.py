from typing import Optional, Dict, Any

from lumigo_tracer.event.trigger_parsing.event_trigger_base import (
    EventTriggerParser,
    ExtraKeys,
    TriggerType,
)


class ApiGatewayEventTriggerParser(EventTriggerParser):
    @staticmethod
    def _should_handle(event: Dict[Any, Any]) -> bool:
        return (
            "httpMethod" in event
            and "headers" in event
            and "requestContext" in event
            and event.get("requestContext", {}).get("elb") is None
            and event.get("requestContext", {}).get("stage") is not None
        ) or (event.get("version", "") == "2.0" and "headers" in event)

    @staticmethod
    def handle(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        version = event.get("version")
        if version and version.startswith("2.0"):
            return ApiGatewayEventTriggerParser._parse_http_method_v2(event, target_id)
        return ApiGatewayEventTriggerParser._parse_http_method_v1(event, target_id)

    @staticmethod
    def _parse_http_method_v1(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        extra = {
            ExtraKeys.HTTP_METHOD: event.get("httpMethod", ""),
            ExtraKeys.RESOURCE: event.get("resource", ""),
        }
        if isinstance(event.get("headers"), dict):
            extra[ExtraKeys.API] = event["headers"].get("Host", "unknown.unknown.unknown")
        if isinstance(event.get("requestContext"), dict):
            extra[ExtraKeys.STAGE] = event["requestContext"].get("stage", "unknown")
        return EventTriggerParser.build_trigger(
            target_id=target_id,
            resource_type="apigw",
            from_message_ids=[event.get("requestContext", {}).get("requestId", "")],
            extra=extra,
        )

    @staticmethod
    def _parse_http_method_v2(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        extra = {
            ExtraKeys.HTTP_METHOD: event.get("requestContext", {}).get("http", {}).get("method"),
            ExtraKeys.RESOURCE: event.get("requestContext", {}).get("http", {}).get("path"),
            ExtraKeys.API: event.get("requestContext", {}).get("domainName", ""),
            ExtraKeys.STAGE: event.get("requestContext", {}).get("stage", "unknown"),
        }
        return EventTriggerParser.build_trigger(
            target_id=target_id,
            resource_type="apigw",
            from_message_ids=[event.get("requestContext", {}).get("requestId", "")],
            extra=extra,
        )
