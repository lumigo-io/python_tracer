from lumigo_tracer.parsers.parser import ServerlessAWSParser, Parser
import http.client


def test_serverless_aws_parser_fallback_doesnt_change():
    url = "https://kvpuorrsqb.execute-api.us-west-2.amazonaws.com"
    headers = http.client.HTTPMessage()
    headers.add_header("nothing", "relevant")
    serverless_parser = ServerlessAWSParser().parse_response(url, 200, headers=headers, body=b"")
    root_parser = Parser().parse_response(url, 200, headers=headers, body=b"")
    assert serverless_parser == root_parser
