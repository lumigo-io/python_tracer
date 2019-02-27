import json
import logging
import os
import urllib.request
from typing import Union, List

EDGE_HOST = "{region}.tracer-edge.golumigo.com"
LOG_FORMAT = "#LUMIGO# - %(asctime)s - %(levelname)s - %(message)s"
SHOULD_REPORT = False

_connection = None
_HOST: str = ""
_TOKEN = "t_b8a1fcfe9b4d092b50b0"
_logger = None


def config(edge_host: str = "", should_report: Union[bool, None] = None, token: str = None) -> None:
    """
    This function configure the lumigo wrapper.

    :param edge_host: The host to send the events. Leave empty for default.
    :param should_report: Weather we should send the events. Change to True in the production.
    :param token: The token to use when sending back the events.
    """
    # TODO - decide on a real way to config the lambda
    global _HOST, SHOULD_REPORT, _TOKEN
    if edge_host:
        _HOST = edge_host
    if should_report is not None:
        SHOULD_REPORT = should_report
    if token:
        _TOKEN = token


def report_json(region: Union[None, str], msgs: List[dict]) -> None:
    """
    This function sends the information back to the edge.

    :param region: The region to use as default if not configured otherwise.
    :param msgs: the message to send.
    """
    for msg in msgs:
        msg["token"] = _TOKEN
    get_logger().info(f"reporting the messages: {msgs}")
    host = _HOST or EDGE_HOST.format(region=region)
    if SHOULD_REPORT:
        try:
            response = urllib.request.urlopen(
                urllib.request.Request(
                    host, json.dumps(msgs).encode(), headers={"Content-Type": "application/json"}
                )
            )
            get_logger().info(f"successful reporting, code: {getattr(response, 'code', 'unknown')}")
        except Exception as e:
            get_logger().exception(f"could not report json to {host}", exc_info=e)


def get_logger():
    """
    This function returns lumigo's logger.
    The logger streams the logs to the stderr in format the explicitly say that those are lumigo's logs.

    This logger is off by default.
    Add the environment variable `LUMIGO_DEBUG=true` to activate it.
    """
    global _logger
    if not _logger:
        _logger = logging.getLogger("lumigo")
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        if os.environ.get("LUMIGO_DEBUG", "").lower() == "true":
            _logger.setLevel(logging.DEBUG)
        else:
            _logger.setLevel(logging.CRITICAL)
        _logger.addHandler(handler)
    return _logger
