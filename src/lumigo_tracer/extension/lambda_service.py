from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Generator, Optional

from lumigo_tracer.extension.extension_utils import get_extension_logger, request_event


_lambda_service: Optional["LambdaService"] = None


class LambdaService:
    def __init__(self, extension_id):
        self.extension_id: str = extension_id
        self.thread: ThreadPoolExecutor = ThreadPoolExecutor()
        self.next_event_future = None

    def ready_for_next_event(self):
        if not self.next_event_future:
            self.next_event_future = self.thread.submit(request_event, self.extension_id)

    def block_until_next_event(self) -> Dict[str, str]:
        self.ready_for_next_event()
        result = self.next_event_future.result()
        self.next_event_future = None
        return result

    def events_generator(self) -> Generator[Dict[str, str], None, None]:
        try:
            while True:
                yield self.block_until_next_event()
        except Exception:
            get_extension_logger().exception("Extension:: failed retrieving events")
