import json
import logging
import os
import urllib.request
from urllib.error import URLError
from typing import Union, List
from contextlib import contextmanager


EDGE_HOST = "https://{region}.lumigo-tracer-edge.golumigo.com/api/spans"
LOG_FORMAT = "#LUMIGO# - %(asctime)s - %(levelname)s - %(message)s"
_SHOULD_REPORT = True
SECONDS_TO_TIMEOUT = 0.3

_HOST: str = ""
_TOKEN: str = ""
_VERBOSE: bool = True
_logger: Union[logging.Logger, None] = None


def config(
    edge_host: str = "",
    should_report: Union[bool, None] = None,
    token: str = None,
    verbose: bool = True,
) -> None:
    """
    This function configure the lumigo wrapper.

    :param verbose: Whether the tracer should send all the possible information (debug mode)
    :param edge_host: The host to send the events. Leave empty for default.
    :param should_report: Weather we should send the events. Change to True in the production.
    :param token: The token to use when sending back the events.
    """
    global _HOST, _SHOULD_REPORT, _TOKEN, _VERBOSE
    if edge_host:
        _HOST = edge_host
    if should_report is not None:
        _SHOULD_REPORT = should_report
    if token:
        _TOKEN = token
    if not verbose or os.environ.get("LUMIGO_VERBOSE", "").lower() == "false":
        _VERBOSE = False
    else:
        _VERBOSE = True


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
    if _SHOULD_REPORT:
        try:
            response = urllib.request.urlopen(
                urllib.request.Request(
                    host, json.dumps(msgs).encode(), headers={"Content-Type": "application/json"}
                ),
                timeout=float(os.environ.get("LUMIGO_EDGE_TIMEOUT", SECONDS_TO_TIMEOUT)),
            )
            get_logger().info(f"successful reporting, code: {getattr(response, 'code', 'unknown')}")
        except URLError as e:
            get_logger().exception(f"Timeout when reporting to {host}", exc_info=e)
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


def is_verbose():
    return _VERBOSE


@contextmanager
def lumigo_safe_execute(part_name=""):
    try:
        yield
    except Exception as e:
        get_logger().exception(f"An exception occurred in lumigo's code {part_name}", exc_info=e)
