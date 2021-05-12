import re
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


def get_current_memory() -> Optional[float]:
    with lumigo_safe_execute("Extension: get meminfo"):
        with open("/proc/meminfo", "r") as meminfo:
            res = 0.0
            meminfo_content = meminfo.read()
            mem_available_pattern = re.compile(r"(MemAvailable)[:][ ]*([0-9]*)")
            mem_total_pattern = re.compile(r"(MemTotal)[:][ ]*([0-9]*)")

            found_mem_available = re.search(mem_available_pattern, meminfo_content)
            found_mem_total = re.search(mem_total_pattern, meminfo_content)

            if found_mem_total and found_mem_available:
                mem_total = float(found_mem_total.group(2))
                mem_available = float(found_mem_available.group(2))
                res = mem_available / mem_total
        return res


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
