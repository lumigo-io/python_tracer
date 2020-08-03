import json
import re
import urllib.parse
from typing import Tuple, Dict, Union, List, Any, Optional

from lumigo_tracer.libs import xmltodict
import functools
import itertools
from collections.abc import Iterable

from lumigo_tracer.utils import (
    Configuration,
    LUMIGO_EVENT_KEY,
    STEP_FUNCTION_UID_KEY,
    lumigo_safe_execute,
    get_logger,
    md5hash,
)

MESSAGE_ID_KEY = "messageId"
MESSAGE_IDS_KEY = "messageIds"
TRIGGER_CREATION_TIME_KEY = "approxEventCreationTime"


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


def safe_get_list(lst: list, index: Union[int, str], default=None):
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


def recursive_json_join(d1: Optional[dict], d2: Optional[dict]):
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
            d[key] = recursive_json_join(d1.get(key), d2.get(key))  # type: ignore
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
    with lumigo_safe_execute("triggered by"):
        if not isinstance(event, dict):
            return None
        if _is_supported_http_method(event):
            return parse_http_method(event)
        elif _is_supported_sns(event):
            return _parse_sns(event)
        elif _is_supported_streams(event):
            return _parse_streams(event)
        elif _is_supported_cw(event):
            return _parse_cw(event)
        elif _is_step_function(event):
            return _parse_step_function(event)

    return _parse_unknown(event)


def _parse_unknown(event: dict):
    result = {"triggeredBy": "unknown"}
    return result


def _is_step_function(event):
    return Configuration.is_step_function and STEP_FUNCTION_UID_KEY in event.get(
        LUMIGO_EVENT_KEY, {}
    )


def _parse_step_function(event: dict):
    result = {
        "triggeredBy": "stepFunction",
        "messageId": event[LUMIGO_EVENT_KEY][STEP_FUNCTION_UID_KEY],
    }
    return result


def _is_supported_http_method(event: dict):
    return (
        "httpMethod" in event  # noqa
        and "headers" in event  # noqa
        and "requestContext" in event  # noqa
        and event.get("requestContext", {}).get("elb") is None  # noqa
    ) or (  # noqa
        event.get("version", "") == "2.0" and "headers" in event  # noqa
    )  # noqa  # noqa


def parse_http_method(event: dict):
    version = event.get("version")
    if version and version.startswith("2.0"):
        return _parse_http_method_v2(event)
    return _parse_http_method_v1(event)


def _parse_http_method_v1(event: dict):
    result = {
        "triggeredBy": "apigw",
        "httpMethod": event.get("httpMethod", ""),
        "resource": event.get("resource", ""),
        "messageId": event.get("requestContext", {}).get("requestId", ""),
    }
    if isinstance(event.get("headers"), dict):
        result["api"] = event["headers"].get("Host", "unknown.unknown.unknown")
    if isinstance(event.get("requestContext"), dict):
        result["stage"] = event["requestContext"].get("stage", "unknown")
    return result


def _parse_http_method_v2(event: dict):
    result = {
        "triggeredBy": "apigw",
        "httpMethod": event.get("requestContext", {}).get("http", {}).get("method"),
        "resource": event.get("requestContext", {}).get("http", {}).get("path"),
        "messageId": event.get("requestContext", {}).get("requestId", ""),
        "api": event.get("requestContext", {}).get("domainName", ""),
        "stage": event.get("requestContext", {}).get("stage", "unknown"),
    }
    return result


def _is_supported_sns(event: dict):
    return event.get("Records", [{}])[0].get("EventSource") == "aws:sns"


def _parse_sns(event: dict):
    return {
        "triggeredBy": "sns",
        "arn": event["Records"][0]["Sns"]["TopicArn"],
        "messageId": event["Records"][0]["Sns"].get("MessageId"),
    }


def _is_supported_cw(event: dict):
    return event.get("detail-type") == "Scheduled Event" and "source" in event and "time" in event


def _parse_cw(event: dict):
    resource = event.get("resources", ["/unknown"])[0].split("/")[1]
    return {
        "triggeredBy": "cloudwatch",
        "resource": resource,
        "region": event.get("region"),
        "detailType": event.get("detail-type"),
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
        result.update(_parse_sqs_event(event))
    elif triggered_by == "kinesis":
        result["messageId"] = safe_get(event, ["Records", 0, "kinesis", "sequenceNumber"])
    elif triggered_by == "dynamodb":
        result.update(_parse_dynamomdb_event(event))
    return result


def _get_ddb_approx_creation_time_ms(event) -> int:
    return event["Records"][0].get("dynamodb", {}).get("ApproximateCreationDateTime", 0) * 1000


def _parse_dynamomdb_event(event) -> Dict[str, Union[int, List[str]]]:
    creation_time = _get_ddb_approx_creation_time_ms(event)
    mids = []
    for record in event["Records"]:
        event_name = record.get("eventName")
        if event_name in ("MODIFY", "REMOVE") and record.get("dynamodb", {}).get("Keys"):
            mids.append(md5hash(record["dynamodb"]["Keys"]))
        elif event_name == "INSERT" and record.get("dynamodb", {}).get("NewImage"):
            mids.append(md5hash(record["dynamodb"]["NewImage"]))
    return {MESSAGE_IDS_KEY: mids, TRIGGER_CREATION_TIME_KEY: creation_time}


def _parse_sqs_event(event) -> Dict[str, Union[int, List[str]]]:
    mids = [record["messageId"] for record in event["Records"] if record.get("messageId")]
    return {MESSAGE_IDS_KEY: mids} if len(mids) > 1 else {MESSAGE_ID_KEY: mids[0]}


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


def str_to_tuple(val: str) -> Optional[Tuple]:
    try:
        if val:
            return tuple(val.split(","))
    except Exception as e:
        get_logger().debug("Error while convert str to tuple", exc_info=e)
    return None
