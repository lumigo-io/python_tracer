import os
from dataclasses import dataclass, asdict

from typing import Dict, Optional, List, Union
from datetime import datetime

from lumigo_tracer.extension.extension_utils import get_current_bandwidth, get_extension_logger
from lumigo_tracer import lumigo_utils
from lumigo_tracer.extension.lambda_service import LambdaService
from lumigo_tracer.extension.sampler import CpuSampler
from lumigo_tracer.extension.sampler import MemorySampler
from lumigo_tracer.lumigo_utils import lumigo_safe_execute

SPAN_TYPE = "extensionExecutionEnd"


@dataclass
class ExtensionEvent:
    token: str
    started: int
    requestId: str
    networkBytesUsed: int
    cpuUsageTime: List[Dict[str, Union[float, int]]]
    memoryUsageTime: List[Dict[str, Union[float, int]]]
    type: str = SPAN_TYPE


class LumigoExtension:
    def __init__(self, lambda_service: LambdaService):
        self.lambda_service: LambdaService = lambda_service
        self.spu_sampler: CpuSampler = CpuSampler()
        self.memory_sampler: MemorySampler = MemorySampler()
        self.start_time: Optional[datetime] = None
        self.request_id: Optional[str] = None
        self.bandwidth: Optional[int] = None

    def start_new_invocation(self, event: Dict[str, str]):
        with lumigo_safe_execute("Extension: start_new_invocation"):
            current_bandwidth = get_current_bandwidth()
            if self.request_id:
                self._finish_previous_invocation(current_bandwidth)
            self.spu_sampler.start_sampling()
            self.memory_sampler.start_sampling()
            self.lambda_service.ready_for_next_event()
            self.request_id = event.get("requestId")
            self.start_time = datetime.now()
            self.bandwidth = current_bandwidth

    def shutdown(self):
        with lumigo_safe_execute("Extension: shutdown"):
            current_bandwidth = get_current_bandwidth()
            self._finish_previous_invocation(current_bandwidth)

    def _finish_previous_invocation(self, current_bandwidth: Optional[int]):
        self.spu_sampler.stop_sampling()
        self.memory_sampler.stop_sampling()
        token = os.environ.get(lumigo_utils.LUMIGO_TOKEN_KEY)
        if not token:
            get_extension_logger().warning(
                f"Skip sending data: No token was found. Request id: {self.request_id}"
            )
            return
        if not (
            self.request_id
            and self.start_time  # noqa: W503
            and self.spu_sampler.get_samples()  # noqa: W503
            and self.memory_sampler.get_samples()  # noqa: W503
            and self.bandwidth  # noqa: W503
            and current_bandwidth  # noqa: W503
        ):
            get_extension_logger().warning("Skip sending data: unable retrieving all data")
            return
        span = ExtensionEvent(
            token=token,
            started=int(self.start_time.timestamp() * 1000),
            requestId=self.request_id,
            networkBytesUsed=current_bandwidth - self.bandwidth,
            cpuUsageTime=[s.dump() for s in self.spu_sampler.get_samples()],
            memoryUsageTime=[s.dump() for s in self.memory_sampler.get_samples()],
        )
        lumigo_utils.report_json(os.environ.get("AWS_REGION", "us-east-1"), msgs=[asdict(span)])
