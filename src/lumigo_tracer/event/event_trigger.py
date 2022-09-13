import re
from typing import Union, List, Dict

from lumigo_tracer.parsing_utils import recursive_get_key, safe_get, safe_split_get
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
TOTAL_SIZE_BYTES = "totalSizeBytes"
RECORDS_NUM = "recordsNum"
MESSAGE_ID_TO_CHAINED_RESOURCE = "messageIdToChainResource"


def parse_triggered_by(event: dict):  # type: ignore[no-untyped-def,type-arg]
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
        if _is_load_balancer_method(event):
            return _parse_load_balancer_method(event)
        elif _is_supported_sns(event):
            return _parse_sns(event)
        elif _is_supported_streams(event):
            return _parse_streams(event)
        elif _is_supported_cw(event):
            return _parse_cw(event)
        elif _is_step_function(event):
            return _parse_step_function(event)
        elif _is_event_bridge(event):
            return _parse_event_bridge(event)
        elif _is_appsync(event):
            return _parse_appsync(event)

    return _parse_unknown(event)


def _parse_unknown(event: dict):  # type: ignore[no-untyped-def,type-arg]
    result = {"triggeredBy": "unknown"}
    return result


def _is_step_function(event: Union[List, Dict]):  # type: ignore[no-untyped-def,type-arg,type-arg]
    return (
        Configuration.is_step_function
        and isinstance(event, (list, dict))  # noqa
        and STEP_FUNCTION_UID_KEY in recursive_get_key(event, LUMIGO_EVENT_KEY, default={})  # noqa
    )


def _parse_step_function(event: dict):  # type: ignore[no-untyped-def,type-arg]
    result = {
        "triggeredBy": "stepFunction",
        "messageId": recursive_get_key(event, LUMIGO_EVENT_KEY)[STEP_FUNCTION_UID_KEY],
    }
    return result


def _is_supported_http_method(event: dict):  # type: ignore[no-untyped-def,type-arg]
    return (
        "httpMethod" in event  # noqa
        and "headers" in event  # noqa
        and "requestContext" in event  # noqa
        and event.get("requestContext", {}).get("elb") is None  # noqa
        and event.get("requestContext", {}).get("stage") is not None  # noqa
    ) or (  # noqa
        event.get("version", "") == "2.0" and "headers" in event  # noqa
    )  # noqa


def _is_load_balancer_method(event: dict):  # type: ignore[no-untyped-def,type-arg]
    return (
        "httpMethod" in event  # noqa
        and "headers" in event  # noqa
        and event["headers"].get("host")  # noqa
        and "requestContext" in event  # noqa
        and (  # noqa
            event.get("requestContext", {}).get("elb") is not None  # noqa
            or event.get("requestContext", {}).get("alb") is not None  # noqa
        )  # noqa
    )


def _parse_http_method(event: dict):  # type: ignore[no-untyped-def,type-arg]
    version = event.get("version")
    if version and version.startswith("2.0"):
        return _parse_http_method_v2(event)
    return _parse_http_method_v1(event)


def _parse_load_balancer_method(event: dict):  # type: ignore[no-untyped-def,type-arg]
    result = {
        "triggeredBy": "load_balancer",
        "httpMethod": event.get("httpMethod", ""),
    }
    if isinstance(event.get("headers"), dict):
        result["api"] = event["headers"].get("host")
    return result


def _parse_http_method_v1(event: dict):  # type: ignore[no-untyped-def,type-arg]
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


def _parse_http_method_v2(event: dict):  # type: ignore[no-untyped-def,type-arg]
    result = {
        "triggeredBy": "apigw",
        "httpMethod": event.get("requestContext", {}).get("http", {}).get("method"),
        "resource": event.get("requestContext", {}).get("http", {}).get("path"),
        "messageId": event.get("requestContext", {}).get("requestId", ""),
        "api": event.get("requestContext", {}).get("domainName", ""),
        "stage": event.get("requestContext", {}).get("stage", "unknown"),
    }
    return result


def _is_supported_sns(event: dict):  # type: ignore[no-untyped-def,type-arg]
    return event.get("Records", [{}])[0].get("EventSource") == "aws:sns"


def _parse_sns(event: dict):  # type: ignore[no-untyped-def,type-arg]
    return {
        "triggeredBy": "sns",
        "arn": event["Records"][0]["Sns"]["TopicArn"],
        "messageId": event["Records"][0]["Sns"].get("MessageId"),
        RECORDS_NUM: len(event["Records"]),
    }


def _is_event_bridge(event: dict):  # type: ignore[no-untyped-def,type-arg]
    return (
        isinstance(event.get("version"), str)
        and isinstance(event.get("id"), str)  # noqa: W503
        and isinstance(event.get("detail-type"), str)  # noqa: W503
        and isinstance(event.get("source"), str)  # noqa: W503
        and isinstance(event.get("time"), str)  # noqa: W503
        and isinstance(event.get("region"), str)  # noqa: W503
        and isinstance(event.get("resources"), list)  # noqa: W503
        and isinstance(event.get("detail"), dict)  # noqa: W503
    )


def _is_appsync(event: dict) -> bool:  # type: ignore[type-arg]
    host = safe_get(event, ["context", "request", "headers", "host"])
    if not host:
        host = safe_get(event, ["request", "headers", "host"])
    return isinstance(host, str) and "appsync-api" in host


def _parse_event_bridge(event: dict):  # type: ignore[no-untyped-def,type-arg]
    return {"triggeredBy": "eventBridge", "messageId": event["id"]}


def _parse_appsync(event: dict) -> dict:  # type: ignore[type-arg]
    headers = safe_get(event, ["context", "request", "headers"])
    if not headers:
        headers = safe_get(event, ["request", "headers"])
    host = headers.get("host")
    trace_id = headers.get("x-amzn-trace-id")
    message_id = safe_split_get(trace_id, "=", -1)
    return {"triggeredBy": "appsync", "api": host, "messageId": message_id}


def _is_supported_cw(event: dict):  # type: ignore[no-untyped-def,type-arg]
    return event.get("detail-type") == "Scheduled Event" and "source" in event and "time" in event


def _parse_cw(event: dict):  # type: ignore[no-untyped-def,type-arg]
    resource = event.get("resources", ["/unknown"])[0].split("/")[1]
    return {
        "triggeredBy": "cloudwatch",
        "resource": resource,
        "region": event.get("region"),
        "detailType": event.get("detail-type"),
    }


def _is_supported_streams(event: dict):  # type: ignore[no-untyped-def,type-arg]
    return event.get("Records", [{}])[0].get("eventSource") in [
        "aws:kinesis",
        "aws:dynamodb",
        "aws:sqs",
        "aws:s3",
    ]


def _parse_streams(event: dict) -> Dict[str, str]:  # type: ignore[type-arg]
    """
    :return: {"triggeredBy": str, "arn": str}
    If has messageId, return also: {"messageId": str}
    """
    triggered_by = event["Records"][0]["eventSource"].split(":")[1]
    result = {"triggeredBy": triggered_by, RECORDS_NUM: len(event["Records"])}
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
        result.update(_parse_kinesis_event(event))
    elif triggered_by == "dynamodb":
        result.update(_parse_dynamomdb_event(event))
    return result


def _get_ddb_approx_creation_time_ms(event) -> int:  # type: ignore[no-untyped-def]
    return event["Records"][0].get("dynamodb", {}).get("ApproximateCreationDateTime", 0) * 1000  # type: ignore[no-any-return]


def _parse_dynamomdb_event(event) -> Dict[str, Union[int, List[str]]]:  # type: ignore[no-untyped-def]
    creation_time = _get_ddb_approx_creation_time_ms(event)
    mids = []
    total_size_bytes: int = 0
    for record in event["Records"]:
        total_size_bytes += record["dynamodb"].get("SizeBytes", 0)
        event_name = record.get("eventName")
        if event_name in ("MODIFY", "REMOVE") and record.get("dynamodb", {}).get("Keys"):
            mids.append(md5hash(record["dynamodb"]["Keys"]))
        elif event_name == "INSERT" and record.get("dynamodb", {}).get("NewImage"):
            mids.append(md5hash(record["dynamodb"]["NewImage"]))
    return {
        MESSAGE_IDS_KEY: mids,
        TRIGGER_CREATION_TIME_KEY: creation_time,
        TOTAL_SIZE_BYTES: total_size_bytes,
    }


def _parse_kinesis_event(event) -> Dict[str, Union[int, str, List[str], List[Dict[str, str]]]]:  # type: ignore[no-untyped-def]
    result = {}
    message_ids = []
    records = safe_get(event, ["Records"], default=[])
    for record in records:
        message_id = safe_get(record, ["kinesis", "sequenceNumber"])
        if message_id:
            message_ids.append(message_id)
    if message_ids:
        if len(message_ids) == 1:
            result[MESSAGE_ID_KEY] = message_ids[0]
        else:
            result[MESSAGE_IDS_KEY] = message_ids
    event_id = safe_get(event, ["Records", 0, "eventID"])
    if isinstance(event_id, str):
        result["shardId"] = event_id.split(":", 1)[0]
    return result


def _parse_sqs_event(event) -> Dict[str, Union[int, str, List[str], List[Dict[str, str]]]]:  # type: ignore[no-untyped-def]
    message_ids = []
    chained_resources: List[Dict[str, str]] = []
    for record in event.get("Records", []):
        record_message_id = record.get("messageId")
        if not record_message_id:
            continue
        message_ids.append(record_message_id)
        if _is_sns_inside_sqs_record(record):
            body = record.get("body", "")
            message_id = re.search(r'"MessageId" : "(\w{8}-\w{4}-\w{4}-\w{4}-\w{12})"', body)
            topic_arn = re.search(r'"TopicArn" : "(arn:aws:sns:[\w\-:]+)"', body)
            if message_id and topic_arn:
                message_ids.append(message_id.group(1))
                chained_resources.append(
                    {
                        "resourceType": "sns",
                        "TopicArn": topic_arn.group(1),
                        "childMessageId": record_message_id,
                        "parentMessageId": message_id.group(1),
                    }
                )
    result: Dict[str, Union[int, str, List[str], List[Dict[str, str]]]] = {}
    if len(message_ids) > 1:
        result[MESSAGE_IDS_KEY] = message_ids
    else:
        result[MESSAGE_ID_KEY] = message_ids[0]
    if chained_resources:
        result[MESSAGE_ID_TO_CHAINED_RESOURCE] = chained_resources
    return result


def _is_sns_inside_sqs_record(record: dict):  # type: ignore[no-untyped-def,type-arg]
    body = record.get("body")
    return isinstance(body, str) and "SimpleNotificationService" in body and "TopicArn" in body
