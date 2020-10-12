import json
import os
import urllib.request
from typing import Optional, Dict

from lumigo_tracer.lumigo_utils import get_logger, lumigo_safe_execute


def get_current_cpu_time() -> Optional[int]:
    """
    :return: the total number of milliseconds that being used by the CPU.
    """
    with lumigo_safe_execute("Extension: get cpu time"):
        total = 0
        with open("/proc/stat", "r") as stats:
            for line in stats.readlines():
                if line.startswith("cpu "):
                    parts = line.split()
                    total += (int(parts[1]) + int(parts[3])) * 10
        return total


def get_current_bandwidth() -> Optional[int]:
    with lumigo_safe_execute("Extension: get bandwidth"):
        with open("/proc/net/netstat", "r") as stats:
            last = stats.read().splitlines()[-1]
            parts = last.split()
        return int(parts[7]) + int(parts[8])


def request_event(extension_id: str) -> Dict[str, str]:
    url = f"http://{os.environ['AWS_LAMBDA_RUNTIME_API']}/2020-01-01/extension/event/next"
    headers = {"Lambda-Extension-Identifier": extension_id}
    return json.loads(urllib.request.urlopen(urllib.request.Request(url, headers=headers)).read())


def get_extension_logger():
    return get_logger("lumigo-extension")
