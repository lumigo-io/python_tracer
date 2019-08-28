import json

from lumigo_tracer.parsers.parser import ServerlessAWSParser, Parser
from lumigo_tracer.parsers.http_data_classes import HttpRequest
from lumigo_tracer.utils import Configuration


def test_serverless_aws_parser_fallback_doesnt_change():
    url = "https://kvpuorrsqb.execute-api.us-west-2.amazonaws.com"
    headers = {"nothing": "relevant"}
    serverless_parser = ServerlessAWSParser().parse_response(url, 200, headers=headers, body=b"")
    root_parser = Parser().parse_response(url, 200, headers=headers, body=b"")
    serverless_parser.pop("ended")
    root_parser.pop("ended")
    assert serverless_parser == root_parser


def test_non_decodeable_body(monkeypatch):
    """
    Note: this test may fail only in python2, where '\xff' is an encoded bytes, which can not be cast to any str.
    """
    monkeypatch.setattr(Configuration, "verbose", True)
    params = HttpRequest(host="a", method="b", uri="c", headers={}, body="\xff")

    return_json = Parser().parse_request(params)
    assert json.dumps(return_json)
