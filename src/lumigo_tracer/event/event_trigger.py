from typing import Union, List, Dict

from lumigo_tracer.parsing_utils import recursive_get_key, safe_get
from lumigo_tracer.lumigo_utils import (
    lumigo_safe_execute,
    Configuration,
    STEP_FUNCTION_UID_KEY,
    LUMIGO_EVENT_KEY,
    md5hash,
)

TRIGGER_CREATION_TIME_KEY = "approxEventCreationTime"
MESSAGE_ID_KEY = "messageId"
MESSAGE_IDS_KEY = "messageIds"


def parse_triggered_by(event: dict):
    """
    This function parses the event and build the dictionary that describes the given event.

    The current possible values are:
    * {triggeredBy: unknown}
    * {triggeredBy: apigw, api: <host>, resource: <>, httpMethod: <>, stage: <>, identity: <>, referer: <>}
    """
    with lumigo_safe_execute("triggered by"):
        if not isinstance(event, dict):
            if _is_step_function(event):
                return _parse_step_function(event)
            return None
        if _is_supported_http_method(event):
            return _parse_http_method(event)
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


def _is_step_function(event: Union[List, Dict]):
    return (
        Configuration.is_step_function
        and isinstance(event, (list, dict))  # noqa
        and STEP_FUNCTION_UID_KEY in recursive_get_key(event, LUMIGO_EVENT_KEY, default={})  # noqa
    )


def _parse_step_function(event: dict):
    result = {
        "triggeredBy": "stepFunction",
        "messageId": recursive_get_key(event, LUMIGO_EVENT_KEY)[STEP_FUNCTION_UID_KEY],
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


def _parse_http_method(event: dict):
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
