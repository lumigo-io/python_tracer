import json
import urllib.request

EDGE_HOST = "{region}.tracer-edge.golumigo.com"
SHOULD_REPORT = False

_connection = None
_HOST: str = ""
_TOKEN = "t_b8a1fcfe9b4d092b50b0"


def config(edge_host: str = "", should_report: bool = False, token: str = None) -> None:
    # TODO - decide on a real way to config the lambda
    global _HOST, SHOULD_REPORT, _TOKEN
    if edge_host:
        _HOST = edge_host
    if should_report:
        SHOULD_REPORT = should_report
    if token:
        _TOKEN = token


def report_json(msg: dict) -> None:
    msg["token"] = _TOKEN
    if SHOULD_REPORT:
        # TODO - validate final API - create e2e tests
        urllib.request.urlopen(
            urllib.request.Request(
                _HOST, json.dumps(msg).encode(), headers={"Content-Type": "application/json"}
            )
        )
    else:
        print(msg)
