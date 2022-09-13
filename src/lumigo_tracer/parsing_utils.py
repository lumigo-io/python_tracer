import json
import re
import urllib.parse
from typing import Tuple, Dict, Union, List, Any, Optional

from lumigo_tracer.libs import xmltodict
import functools
import itertools
from collections.abc import Iterable

from lumigo_tracer.lumigo_utils import Configuration, get_logger


def safe_get(d: Union[dict, list], keys: List[Union[str, int]], default: Any = None) -> Any:  # type: ignore[type-arg,type-arg]
    """
    :param d: Should be list or dict, otherwise return default.
    :param keys: If keys[i] is int, then it should be a list index. If keys[i] is string, then it should be a dict key.
    :param default: If encountered a problem, return default.
    :return: d[keys[0]][keys[1]]...
    """

    def get_next_val(prev_result, key):  # type: ignore[no-untyped-def]
        if isinstance(prev_result, dict) and isinstance(key, str):
            return prev_result.get(key, default)
        elif isinstance(prev_result, list) and isinstance(key, int):
            return safe_get_list(prev_result, key, default)
        else:
            return default

    return functools.reduce(get_next_val, keys, d)


def safe_get_list(lst: list, index: Union[int, str], default=None):  # type: ignore[no-untyped-def,type-arg]
    """
    This function return the organ in the `index` place from the given list.
    If this values doesn't exist, return default.
    """
    if isinstance(index, str):
        try:
            index = int(index)
        except ValueError:
            return default
    if not isinstance(lst, Iterable):
        return default
    return lst[index] if len(lst) > index else default


def safe_split_get(string: str, sep: str, index: int, default=None) -> str:  # type: ignore[no-untyped-def]
    """
    This function splits the given string using the sep, and returns the organ in the `index` place.
    If such index doesn't exist, returns default.
    """
    if not isinstance(string, str):
        return default
    return safe_get_list(string.split(sep), index, default)  # type: ignore[no-any-return]


def safe_key_from_json(json_str: bytes, key: object, default=None) -> Union[str, list]:  # type: ignore[no-untyped-def,type-arg]
    """
    This function tries to read the given str as json, and returns the value of the desired key.
    If the key doesn't found or the input string is not a valid json, returns the default.
    """
    try:
        return json.loads(json_str).get(key, default)  # type: ignore[no-any-return]
    except json.JSONDecodeError:
        return default  # type: ignore[no-any-return]


def safe_key_from_xml(xml_str: bytes, key: str, default=None):  # type: ignore[no-untyped-def]
    """
    This function tries to read the given str as XML, and returns the value of the desired key.
    If the key doesn't found or the input string is not a valid XML, returns the default.

    We accept keys with hierarchy by `/` (i.e. we accept keys with the format `outer/inner`)
    If there are some keys with the same name at the same hierarchy, they can be accessed as index in list,
        e.g: <a><b>val0</b><b>val1</b></a> will be accessed with "a/b/0" or "a/b/1".
    """
    try:
        result = functools.reduce(
            lambda prev, sub_key: safe_get_list(prev, sub_key)
            if isinstance(prev, list)
            else prev.get(sub_key, {}),
            key.split("/"),
            xmltodict.parse(xml_str),
        )
        return result or default
    except xmltodict.expat.ExpatError:
        return default


def safe_key_from_query(body: bytes, key: str, default=None) -> str:  # type: ignore[no-untyped-def]
    """
    This function assumes that the first row in the body is the url arguments.
    We assume that the structure of the parameters is as follow:
    * character-escaped using urllib.quote
    * values separated with '&'
    * each item is <key>=<value>

    Note: This function decode the given body, therefore duplicate it's size. Be aware to use only in resources
            with restricted body length.
    """
    return dict(re.findall(r"([^&]+)=([^&]*)", urllib.parse.unquote(body.decode()))).get(
        key, default
    )


def parse_trace_id(trace_id_str: str) -> Tuple[str, str, str]:
    """
    This function parses the trace_id, and result dictionary the describes the data.
    We assume the following format:
    * values separated with ';'
    * each item is <key>=<value>

    :param trace_id_str: The string that came from the environment variables.
    """
    if not isinstance(trace_id_str, str):
        return "", "", ""
    trace_id_parameters = dict(re.findall(r"([^;]+)=([^;]*)", trace_id_str))
    root = trace_id_parameters.get("Root", "")
    root_end_index = trace_id_str.find(";")
    suffix = trace_id_str[root_end_index:] if ";" in trace_id_str else trace_id_str
    return root, safe_split_get(root, "-", 2, default=""), suffix


def recursive_json_join(d1: Optional[dict], d2: Optional[dict]):  # type: ignore[no-untyped-def,type-arg]
    """
    This function return the recursive joint dictionary, which means that for every (item, key) in the result
     dictionary it holds that:
    * if key in d1 and is not dictionary, then the value is d1[key]
    * if key in d2 and is not dictionary, then the value is d2[key]
    * otherwise, join d1[key] and d2[key]
    """
    if d1 is None or d2 is None:
        return d1 or d2
    d = {}
    for key in set(itertools.chain(d1.keys(), d2.keys())):
        value = d1.get(key, d2.get(key))
        if isinstance(value, dict):
            d[key] = recursive_json_join(d1.get(key), d2.get(key))
        else:
            d[key] = value
    return d


def should_scrub_domain(url: str) -> bool:
    if url and Configuration.domains_scrubber:
        for regex in Configuration.domains_scrubber:
            if regex.match(url):
                return True
    return False


def str_to_list(val: str) -> Optional[List[str]]:
    try:
        if val:
            return val.split(",")
    except Exception as e:
        get_logger().debug("Error while convert str to list", exc_info=e)
    return None


def str_to_tuple(val: str) -> Optional[Tuple]:  # type: ignore[type-arg]
    try:
        if val:
            return tuple(val.split(","))
    except Exception as e:
        get_logger().debug("Error while convert str to tuple", exc_info=e)
    return None


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


def extract_function_name_from_arn(arn: str) -> str:
    return safe_split_get(arn, ":", 6)
