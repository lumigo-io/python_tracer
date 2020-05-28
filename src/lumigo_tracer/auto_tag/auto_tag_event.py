import os
from abc import ABC, abstractmethod
from typing import Dict, List

from lumigo_tracer.user_utils import add_execution_tag
from lumigo_tracer.parsers.utils import str_to_list
from lumigo_tracer.utils import get_logger


AUTO_TAG_API_GW_HEADERS = str_to_list(os.environ.get("LUMIGO_AUTO_TAG_API_GW_HEADERS", "")) or []


class EventAutoTagHandler(ABC):
    @staticmethod
    @abstractmethod
    def is_supported(event) -> bool:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def auto_tag(event) -> Dict:
        raise NotImplementedError()


class ApiGWHandler(EventAutoTagHandler):
    @staticmethod
    def is_supported(event) -> bool:
        if (
            len(AUTO_TGA_API_GW_HEADERS) > 0
            and isinstance(event, Dict)  # noqa
            and event.get("requestContext")  # noqa
            and event.get("requestContext", {}).get("domainName")  # noqa
            and event.get("requestContext")  # noqa
            and event.get("requestContext", {}).get("requestId")  # noqa
        ):
            return True
        return False

    @staticmethod
    def auto_tag(event):
        headers = event.get("headers", [])
        for key in AUTO_TGA_API_GW_HEADERS:
            if key in headers:
                add_execution_tag(key, headers[key])


class AutoTagEvent:
    @staticmethod
    def auto_tag_event(event: Dict = None, handlers: List[EventAutoTagHandler] = None):
        if event:
            handlers = handlers or [ApiGWHandler()]
            for handler in handlers:
                try:
                    if handler.is_supported(event):
                        handler.auto_tag(event)
                except Exception as e:
                    get_logger().debug(
                        f"Error while trying to auto tag with handler {handler.__class__.__name__} event {event}",
                        exc_info=e,
                    )
