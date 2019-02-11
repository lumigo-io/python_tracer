import json
import http.client


HOST = "1.execute-api.eu-west-1.amazonaws.com"
SHOULD_REPORT = False

_connection = None


def config(host=None, should_report=False):
    if host:
        global HOST
        HOST = host
    if should_report:
        global SHOULD_REPORT
        SHOULD_REPORT = should_report


def get_connection() -> http.client.HTTPConnection:
    global _connection
    if not _connection:
        _connection = http.client.HTTPConnection(HOST)
    return _connection


def report_json(msg: dict) -> None:
    if SHOULD_REPORT:
        # TODO - decide on a real API
        get_connection().request("GET", "/", json.dumps(msg))
    else:
        print(msg)
