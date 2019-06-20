import json
import re
import urllib.parse
from typing import Tuple, Dict, Union, List, Any

from lumigo_tracer.libs import xmltodict
import functools
import itertools
from collections.abc import Iterable

MAX_ENTRY_SIZE = 1024


def safe_get(d: Union[dict, list], keys: List[Union[str, int]], default: Any = None) -> Any:
    """
    :param d: Should be list or dict, otherwise return default.
    :param keys: If keys[i] is int, then it should be a list index. If keys[i] is string, then it should be a dict key.
    :param default: If encountered a problem, return default.
    :return: d[keys[0]][keys[1]]...
    """

    def get_next_val(prev_result, key):
        if isinstance(prev_result, dict) and isinstance(key, str):
            return prev_result.get(key, default)
        elif isinstance(prev_result, list) and isinstance(key, int):
            return safe_get_list(prev_result, key, default)
        else:
            return default

    return functools.reduce(get_next_val, keys, d)


def safe_get_list(l: list, index: Union[int, str], default=None):
    """
    This function return the organ in the `index` place from the given list.
    If this values doesn't exist, return default.
    """
    if isinstance(index, str):
        try:
            index = int(index)
        except ValueError:
            return default
    if not isinstance(l, Iterable):
        return default
    return l[index] if len(l) > index else default


def safe_split_get(string: str, sep: str, index: int, default=None) -> str:
    """
    This function splits the given string using the sep, and returns the organ in the `index` place.
    If such index doesn't exist, returns default.
    """
    if not isinstance(string, str):
        return default
    return safe_get_list(string.split(sep), index, default)


def safe_key_from_json(json_str: bytes, key: object, default=None) -> Union[str, list]:
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


def parse_triggered_by(event: dict):
    """
    This function parses the event and build the dictionary that describes the given event.

    The current possible values are:
    * {triggeredBy: unknown}
    * {triggeredBy: apigw, api: <host>, resource: <>, httpMethod: <>, stage: <>, identity: <>, referer: <>}
    """
    if not isinstance(event, dict):
        return None
    if _is_supported_http_method(event):
        return parse_http_method(event)
    elif _is_supported_sns(event):
        return _parse_sns(event)
    elif _is_supported_streams(event):
        return _parse_streams(event)
    else:
        return _parse_unknown(event)


def _parse_unknown(event: dict):
    result = {"triggeredBy": "unknown"}
    return result


def _is_supported_http_method(event: dict):
    return "httpMethod" in event


def parse_http_method(event: dict):
    result = {
        "triggeredBy": "apigw",
        "httpMethod": event.get("httpMethod", ""),
        "resource": event.get("resource", ""),
    }
    if isinstance(event.get("headers"), dict):
        result["api"] = event["headers"].get("Host", "unknown.unknown.unknown")
    if isinstance(event.get("requestContext"), dict):
        result["stage"] = event["requestContext"].get("stage", "unknown")
    return result


def _is_supported_sns(event: dict):
    return event.get("Records", [{}])[0].get("EventSource") == "aws:sns"


def _parse_sns(event: dict):
    return {
        "triggeredBy": "sns",
        "arn": event["Records"][0]["Sns"]["TopicArn"],
        "messageId": event["Records"][0]["Sns"].get("MessageId"),
    }


def _is_supported_streams(event: dict):
    return event.get("Records", [{}])[0].get("eventSource") in [
        "aws:kinesis",
        "aws:dynamodb",
        "aws:sqs",
        "aws:s3",
    ]


def _parse_streams(event: dict) -> Dict[str, str]:
    """
    :return: {"triggeredBy": str, "arn": str}
    If has messageId, return also: {"messageId": str}
    """
    triggered_by = event["Records"][0]["eventSource"].split(":")[1]
    result = {"triggeredBy": triggered_by}
    if triggered_by == "s3":
        result["arn"] = event["Records"][0]["s3"]["bucket"]["arn"]
        result["messageId"] = (
            event["Records"][0].get("responseElements", {}).get("x-amz-request-id")
        )
    else:
        result["arn"] = event["Records"][0]["eventSourceARN"]
    if triggered_by == "sqs":
        result["messageId"] = event["Records"][0].get("messageId")
    elif triggered_by == "kinesis":
        result["messageId"] = safe_get(event, ["Records", 0, "kinesis", "sequenceNumber"])
    return result


def prepare_large_data(value: Union[str, bytes, dict], max_size=MAX_ENTRY_SIZE) -> str:
    """
    This function prepare the given value to send it to lumigo.
    You should call to this function if there's a possibility that the value will be big.

    Current logic:
        Converts the data to str and if it is larger than `max_size`, we truncate it.

    :param value: The value we wish to send
    :param max_size: The maximum size of the data that we will send
    :return: The value that we will actually send
    """
    if isinstance(value, dict):
        try:
            value = json.dumps(value)
        except Exception:
            pass
    elif isinstance(value, bytes):
        try:
            value = value.decode()
        except Exception:
            pass

    res = str(value)
    if len(res) > max_size:
        return f"{res[:max_size]}...[too long]"
    return res
