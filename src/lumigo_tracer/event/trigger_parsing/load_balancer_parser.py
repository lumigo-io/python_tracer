from typing import Optional, Dict, Any

from lumigo_tracer.event.trigger_parsing.event_trigger_base import (
    EventTriggerParser,
    ExtraKeys,
    TriggerType,
)


class LoadBalancerEventTriggerParser(EventTriggerParser):
    @staticmethod
    def _should_handle(event: Dict[Any, Any]) -> bool:
        return (
            "httpMethod" in event  # noqa
            and "headers" in event  # noqa
            and event["headers"].get("host")  # noqa
            and "requestContext" in event  # noqa
            and (  # noqa
                event.get("requestContext", {}).get("elb") is not None  # noqa
                or event.get("requestContext", {}).get("alb") is not None  # noqa
            )  # noqa
        )

    @staticmethod
    def handle(event: Dict[Any, Any], target_id: Optional[str]) -> TriggerType:
        extra = {ExtraKeys.HTTP_METHOD: event.get("httpMethod", "")}
        if isinstance(event.get("headers"), dict):
            extra[ExtraKeys.API] = event["headers"].get("host")
        return EventTriggerParser.build_trigger(
            target_id=target_id,
            resource_type="load_balancer",
            extra=extra,
        )
