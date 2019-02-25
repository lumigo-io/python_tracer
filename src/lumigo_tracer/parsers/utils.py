import json
import re
import urllib.parse
from typing import Tuple

from lumigo_tracer.libs import xmltodict
import functools
import itertools


def safe_get(l: list, index: int, default=None):
    """
    This function return the organ in the `index` place from the given list.
    If this values doesn't exist, return default.
    """
    return l[index] if len(l) > index else default


def safe_split_get(string: str, sep: str, index: int, default=None) -> str:
    """
    This function splits the given string using the sep, and returns the organ in the `index` place.
    If such index doesn't exist, returns default.
    """
    if not isinstance(string, str):
        return default
    return safe_get(string.split(sep), index, default)


def safe_key_from_json(json_str: bytes, key: object, default=None) -> str:
    """
    This function tries to read the given str as json, and returns the value of the desired key.
    If the key doesn't found or the input string is not a valid json, returns the default.
    """
    try:
        return json.loads(json_str).get(key, default)
    except json.JSONDecodeError:
        return default


def safe_key_from_xml(xml_str: bytes, key: str, default=None):
    """
    This function tries to read the given str as XML, and returns the value of the desired key.
    If the key doesn't found or the input string is not a valid XML, returns the default.

    We accept keys with hierarchy by `/` (i.e. we accept keys with the format `outer/inner`)
    """
    try:
        result = functools.reduce(
            lambda prev, sub_key: prev.get(sub_key, {}), key.split("/"), xmltodict.parse(xml_str)
        )
        return result or default
    except xmltodict.expat.ExpatError:
        return default


def safe_key_from_query(body: bytes, key: str, default=None) -> str:
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


def recursive_json_join(d1: dict, d2: dict):
    """
    This function return the recursive joint dictionary, which means that for every (item, key) in the result
     dictionary it holds that:
    * if key in d1 and is not dictionary, then the value is d1[key]
    * if key in d2 and is not dictionary, then the value is d2[key]
    * otherwise, join d1[key] and d2[key]
    """
    d = {}
    for key in itertools.chain(d1.keys(), d2.keys()):
        value = d1.get(key, d2.get(key))
        if isinstance(value, dict):
            d[key] = recursive_json_join(d1.get(key, {}), d2.get(key, {}))
        else:
            d[key] = value
    return d
