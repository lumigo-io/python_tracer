from typing import Dict, List, Union

from lumigo_tracer.lumigo_utils import Configuration


def should_scrub_domain(url: str) -> bool:
    if url and Configuration.domains_scrubber:
        if Configuration.domains_scrubber.match(url):
            return True
    return False


def recursive_get_key(d: Union[List, Dict[str, Union[Dict, str]]], key, depth=None, default=None):  # type: ignore[no-untyped-def,type-arg,type-arg]
    if depth is None:
        depth = Configuration.get_key_depth
    if depth == 0:
        return default
    if key in d:
        return d[key]
    if isinstance(d, list):
        for v in d:
            recursive_result = recursive_get_key(v, key, depth - 1, default)
            if recursive_result:
                return recursive_result
    if isinstance(d, dict):
        for v in d.values():
            if isinstance(v, (list, dict)):
                recursive_result = recursive_get_key(v, key, depth - 1, default)
                if recursive_result:
                    return recursive_result
    return default
