from lumigo_tracer.parsers.parser import ServerlessAWSParser, Parser, get_parser, ApiGatewayV2Parser
import http.client


def test_serverless_aws_parser_fallback_doesnt_change():
    url = "https://kvpuorrsqb.execute-api.us-west-2.amazonaws.com"
    headers = http.client.HTTPMessage()
    headers.add_header("nothing", "relevant")
    serverless_parser = ServerlessAWSParser().parse_response(url, 200, headers=headers, body=b"")
    root_parser = Parser().parse_response(url, 200, headers=headers, body=b"")
    serverless_parser.pop("ended")
    root_parser.pop("ended")
    assert serverless_parser == root_parser


def test_get_parser_check_headers():
    url = "api.rti.dev.toyota.com"
    headers = http.client.HTTPMessage()
    headers.add_header("x-amzn-requestid", "1234")
    assert get_parser(url, headers) == ServerlessAWSParser


def test_get_parser_apigw():
    url = "https://ne3kjv28fh.execute-api.us-west-2.amazonaws.com/doriaviram"
    headers = http.client.HTTPMessage()
    assert get_parser(url, headers) == ApiGatewayV2Parser


def test_apigw_parse_response():
    parser = ApiGatewayV2Parser()
    headers = http.client.HTTPMessage()
    headers.add_header("Apigw-Requestid", "LY_66j0dPHcESCg=")

    result = parser.parse_response("dummy", 200, headers, body=b"")

    assert result["info"] == {
        "messageId": "LY_66j0dPHcESCg=",
        "httpInfo": {
            "host": "dummy",
            "response": {
                "headers": '{"Apigw-Requestid": "LY_66j0dPHcESCg="}',
                "body": "",
                "statusCode": 200,
            },
        },
    }


def test_apigw_parse_response_with_aws_request_id():
    parser = ApiGatewayV2Parser()
    headers = http.client.HTTPMessage()
    headers.add_header("Apigw-Requestid", "LY_66j0dPHcESCg=")
    headers.add_header("x-amzn-RequestId", "x-amzn-RequestId_LY_66j0dPHcESCg=")

    result = parser.parse_response("dummy", 200, headers, body=b"")

    assert result["info"] == {
        "messageId": "x-amzn-RequestId_LY_66j0dPHcESCg=",
        "httpInfo": {
            "host": "dummy",
            "response": {
                "headers": '{"Apigw-Requestid": "LY_66j0dPHcESCg=", "x-amzn-RequestId": "x-amzn-RequestId_LY_66j0dPHcESCg="}',
                "body": "",
                "statusCode": 200,
            },
        },
    }
