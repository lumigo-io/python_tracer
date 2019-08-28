from lumigo_tracer.parsers.parser import ServerlessAWSParser, Parser


def test_serverless_aws_parser_fallback_doesnt_change():
    url = "https://kvpuorrsqb.execute-api.us-west-2.amazonaws.com"
    headers = {"nothing": "relevant"}
    serverless_parser = ServerlessAWSParser().parse_response(url, 200, headers=headers, body=b"")
    root_parser = Parser().parse_response(url, 200, headers=headers, body=b"")
    serverless_parser.pop("ended")
    root_parser.pop("ended")
    assert serverless_parser == root_parser
