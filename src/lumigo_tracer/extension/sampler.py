from datetime import datetime

from typing import Optional, List, Union, Dict

from dataclasses import dataclass

import signal

from lumigo_tracer.extension.extension_utils import get_current_cpu_time
from lumigo_tracer.extension.extension_utils import get_current_memory

DEFAULT_SAMPLING_INTERVAL = 500


@dataclass
class CpuSample:
    start_time: datetime
    end_time: datetime
    cpu_time: float

    def dump(self) -> Dict[str, Union[float, int]]:
        return {
            "start_time": int(self.start_time.timestamp() * 1000),
            "end_time": int(self.end_time.timestamp() * 1000),
            "cpu_time": self.cpu_time,
        }


@dataclass
class MemorySample:
    memory_usage: int
    timestamp: datetime

    def dump(self) -> Dict[str, int]:
        return {
            "memory_usage": self.memory_usage,
            "timestamp": int(self.timestamp.timestamp() * 1000),
        }


class Sampler:
    def __init__(self):
        self.cpu_last_sample_value: Optional[float] = None
        self.cpu_last_sample_time: Optional[datetime] = None
        self.cpu_samples: List[CpuSample] = []
        self.memory_samples: List[MemorySample] = []

    def start_sampling(self, interval_ms: int = DEFAULT_SAMPLING_INTERVAL):
        self.cpu_samples = []
        self.memory_samples = []
        self.sample()
        signal.signal(signal.SIGALRM, self.sample)
        signal.setitimer(signal.ITIMER_REAL, interval_ms / 1000, interval_ms / 1000)

    def stop_sampling(self):
        signal.alarm(0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)
        self.sample()

    def get_cpu_samples(self) -> List[CpuSample]:
        return self.cpu_samples

    def get_memory_samples(self) -> List[MemorySample]:
        return self.memory_samples

    def sample(self, *args):
        now = datetime.now()
        current_cpu = get_current_cpu_time()
        current_memory = get_current_memory()
        if self.cpu_last_sample_time and self.cpu_last_sample_value and current_cpu:
            self.cpu_samples.append(
                CpuSample(
                    start_time=self.cpu_last_sample_time,
                    end_time=now,
                    cpu_time=current_cpu - self.cpu_last_sample_value,
                )
            )
        self.memory_samples.append(MemorySample(memory_usage=current_memory, timestamp=now))
        self.cpu_last_sample_time = now
        self.cpu_last_sample_value = current_cpu
