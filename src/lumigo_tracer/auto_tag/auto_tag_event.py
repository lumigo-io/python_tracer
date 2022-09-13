import os
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

from lumigo_tracer.user_utils import add_execution_tag
from lumigo_tracer.parsing_utils import str_to_list, safe_get
from lumigo_tracer.lumigo_utils import get_logger, is_api_gw_event, Configuration, warn_client

AUTO_TAG_API_GW_HEADERS: Optional[List[str]] = (
    str_to_list(os.environ.get("LUMIGO_AUTO_TAG_API_GW_HEADERS", "")) or []
)


class EventAutoTagHandler(ABC):
    """
    EventAutoTagHandler API
    When adding a new handler update the handlers list under AutoTagEvent.auto_tag_event
    """

    @staticmethod
    @abstractmethod
    def is_supported(event) -> bool:  # type: ignore[no-untyped-def]
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def auto_tag(event) -> Dict:  # type: ignore[no-untyped-def,type-arg]
        raise NotImplementedError()


class ApiGWHandler(EventAutoTagHandler):
    @staticmethod
    def is_supported(event) -> bool:  # type: ignore[no-untyped-def]
        if (
            AUTO_TAG_API_GW_HEADERS
            and len(AUTO_TAG_API_GW_HEADERS) > 0  # noqa
            and is_api_gw_event(event=event)  # noqa
        ):
            return True
        return False

    @staticmethod
    def auto_tag(event: dict):  # type: ignore[no-untyped-def,type-arg]
        if AUTO_TAG_API_GW_HEADERS:
            headers = event.get("headers", [])
            for key in AUTO_TAG_API_GW_HEADERS:
                if key in headers:
                    add_execution_tag(key, headers[key])


class ConfigurationHandler(EventAutoTagHandler):
    @staticmethod
    def is_supported(event: dict) -> bool:  # type: ignore[type-arg]
        return bool(event) and any(key.split(".")[0] in event for key in Configuration.auto_tag)

    @staticmethod
    def auto_tag(event: dict):  # type: ignore[no-untyped-def,type-arg]
        for key in Configuration.auto_tag:
            try:
                value = safe_get(event, key.split("."))  # type: ignore[arg-type]
                if value:
                    add_execution_tag(key, value)
            except Exception as err:
                warn_client(f"Failed to auto tag key {key}: {err}")


class AutoTagEvent:
    @staticmethod
    def auto_tag_event(
        event: Optional[Dict] = None, handlers: Optional[List[EventAutoTagHandler]] = None  # type: ignore[type-arg]
    ) -> None:
        if event:
            handlers = handlers or [ApiGWHandler(), ConfigurationHandler()]
            for handler in handlers:
                try:
                    if handler.is_supported(event):
                        handler.auto_tag(event)
                except Exception as e:
                    get_logger().debug(
                        f"Error while trying to auto tag with handler {handler.__class__.__name__} event {event}",
                        exc_info=e,
                    )
