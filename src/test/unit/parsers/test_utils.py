import os

import pytest

from lumigo_tracer.parsers import utils
from lumigo_tracer.parsers.utils import should_scrub_domain
from lumigo_tracer.utils import config, Configuration


@pytest.mark.parametrize(
    ("input_params", "expected_output"),
    [
        (("a.b.c", ".", 0), "a"),  # happy flow
        (("a.b.c", ".", 1), "b"),
        (("a.b.c", ".", 5, "d"), "d"),  # return the default
    ],
)
def test_safe_split_get(input_params, expected_output):
    assert utils.safe_split_get(*input_params) == expected_output


@pytest.mark.parametrize(
    ("input_params", "expected_output"),
    [
        ((b'{"a": "b"}', "a"), "b"),  # happy flow
        ((b'{"a": "b"}', "c"), None),  # return the default
        ((b"<a>b</a>", "c"), None),  # not a json
    ],
)
def test_key_from_json(input_params, expected_output):
    assert utils.safe_key_from_json(*input_params) == expected_output


@pytest.mark.parametrize(
    ("input_params", "expected_output"),
    [
        ((b"<a>b</a>", "a"), "b"),  # happy flow - one parameter
        ((b"<a><b>c</b><d></d></a>", "a/b"), "c"),  # happy flow - longer path
        ((b"<a>b</a>", "c"), None),  # not existing key
        ((b"<a><b>c</b></a>", "a/e"), None),  # not existing sub-key
        ((b'{"a": "b"}', "c"), None),  # not an xml
    ],
)
def test_key_from_xml(input_params, expected_output):
    assert utils.safe_key_from_xml(*input_params) == expected_output


@pytest.mark.parametrize(
    ("input_params", "expected_output"),
    [
        ((b"a=b", "a"), "b"),  # happy flow - one parameter
        ((b"a=b&c=d", "c"), "d"),  # happy flow - multiple parameters
        ((b"a=b&c=d", "e"), None),  # not existing key
        ((b'{"a": "b"}', "c"), None),  # not an query, no '&'
        ((b"a&b", "a"), None),  # not an query, with '&'
    ],
)
def test_key_from_query(input_params, expected_output):
    assert utils.safe_key_from_query(*input_params) == expected_output


@pytest.mark.parametrize(
    ("trace_id", "result"),
    [
        ("Root=1-2-3;Parent=34;Sampled=0", ("1-2-3", "3", ";Parent=34;Sampled=0")),  # happy flow
        ("Root=1-2-3;", ("1-2-3", "3", ";")),
        ("Root=1-2;", ("1-2", "", ";")),
        ("a;1", ("", "", ";1")),
        ("123", ("", "", "123")),
    ],
)
def test_parse_trace_id(trace_id, result):
    assert utils.parse_trace_id(trace_id) == result


@pytest.mark.parametrize(
    ("d1", "d2", "result"),
    [
        ({1: 2}, {3: 4}, {1: 2, 3: 4}),  # happy flow
        ({1: 2}, {1: 3}, {1: 2}),  # same key twice
        ({1: {2: 3}}, {4: 5}, {1: {2: 3}, 4: 5}),  # dictionary in d1 and nothing in d2
        ({1: {2: 3}}, {1: {4: 5}}, {1: {2: 3, 4: 5}}),  # merge two inner dictionaries
    ],
)
def test_recursive_json_join(d1, d2, result):
    assert utils.recursive_json_join(d1, d2) == result


@pytest.mark.parametrize(
    ("event", "output"),
    [
        (  # apigw example trigger
            {
                "httpMethod": "GET",
                "resource": "resource",
                "headers": {"Host": "www.google.com"},
                "requestContext": {"stage": "1", "requestId": "123"},
            },
            {
                "triggeredBy": "apigw",
                "httpMethod": "GET",
                "api": "www.google.com",
                "stage": "1",
                "resource": "resource",
                "messageId": "123",
            },
        ),
        (  # sns example trigger
            {
                "Records": [
                    {
                        "EventSource": "aws:sns",
                        "Sns": {
                            "TopicArn": "arn:aws:sns:us-east-1:123456789:sns-topic-name",
                            "MessageId": "9cecb7e5-b11e-59fa-95c8-e28d3f64d6a8",
                        },
                    }
                ]
            },
            {
                "triggeredBy": "sns",
                "arn": "arn:aws:sns:us-east-1:123456789:sns-topic-name",
                "messageId": "9cecb7e5-b11e-59fa-95c8-e28d3f64d6a8",
            },
        ),
        (  # s3 example trigger
            {
                "Records": [
                    {
                        "s3": {"bucket": {"arn": "arn:aws:s3:::s3-bucket-name"}},
                        "awsRegion": "us-east-1",
                        "eventName": "ObjectCreated:Put",
                        "eventSource": "aws:s3",
                        "responseElements": {"x-amz-request-id": "E6CFE6C141196902"},
                    }
                ]
            },
            {
                "triggeredBy": "s3",
                "arn": "arn:aws:s3:::s3-bucket-name",
                "messageId": "E6CFE6C141196902",
            },
        ),
        (  # kinesis example trigger
            {
                "Records": [
                    {
                        "eventSourceARN": "arn:aws:kinesis:us-east-1:123456789:stream/kinesis-stream-name",
                        "eventSource": "aws:kinesis",
                        "kinesis": {"sequenceNumber": "12"},
                    }
                ]
            },
            {
                "triggeredBy": "kinesis",
                "arn": "arn:aws:kinesis:us-east-1:123456789:stream/kinesis-stream-name",
                "messageId": "12",
            },
        ),
        (  # DynamoDB example trigger
            {
                "Records": [
                    {
                        "eventSourceARN": "arn:aws:dynamodb:us-east-1:123456789:table/dynamodb-table-name",
                        "eventSource": "aws:dynamodb",
                    }
                ]
            },
            {
                "triggeredBy": "dynamodb",
                "arn": "arn:aws:dynamodb:us-east-1:123456789:table/dynamodb-table-name",
            },
        ),
        (  # SQS example trigger
            {
                "Records": [
                    {
                        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:sqs-queue-name",
                        "eventSource": "aws:sqs",
                        "messageId": "e97ff404-96ca-460e-8ff0-a46012e61826",
                    }
                ]
            },
            {
                "triggeredBy": "sqs",
                "arn": "arn:aws:sqs:us-east-1:123456789:sqs-queue-name",
                "messageId": "e97ff404-96ca-460e-8ff0-a46012e61826",
            },
        ),
        (  # Step Function
            {
                "bla": "saart",
                "_lumigo": {"step_function_uid": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"},
            },
            {"triggeredBy": "stepFunction", "messageId": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"},
        ),
        (
            {
                "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
                "detail-type": "Scheduled Event",
                "source": "aws.events",
                "time": "1970-01-01T00:00:00Z",
                "region": "us-east-1",
                "resources": ["arn:aws:events:us-east-1:123456789012:rule/ExampleRule"],
                "detail": {},
            },
            {
                "triggeredBy": "cloudwatch",
                "resource": "ExampleRule",
                "region": "us-east-1",
                "detailType": "Scheduled Event",
            },
        ),
        (
            {
                "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
                "detail-type": "Unknown",
                "source": "aws.events",
                "time": "1970-01-01T00:00:00Z",
                "region": "us-east-1",
                "resources": ["arn:aws:events:us-east-1:123456789012:rule/ExampleRule"],
                "detail": {},
            },
            {"triggeredBy": "unknown"},
        ),
        (
            {
                "id": "cdc73f9d-aea9-11e3-9d5a-835b769c0d9c",
                "detail-type": "Scheduled Event",
                "source": "aws.events",
                "time": "1970-01-01T00:00:00Z",
                "region": "us-east-1",
                "detail": {},
            },
            {
                "triggeredBy": "cloudwatch",
                "resource": "unknown",
                "region": "us-east-1",
                "detailType": "Scheduled Event",
            },
        ),
        (  # elb example trigger
            {
                "httpMethod": "GET",
                "resource": "resource",
                "headers": {"Host": "www.google.com"},
                "requestContext": {"elb": "Elb", "stage": "1", "requestId": "123"},
            },
            {"triggeredBy": "unknown"},
        ),
        ({"bla": "bla2"}, {"triggeredBy": "unknown"}),  # unknown trigger
        (None, None),
    ],
)
def test_parse_triggered_by(event, output):
    Configuration.is_step_function = True
    assert utils.parse_triggered_by(event) == output


def test_config_with_verbose_param_with_no_env_verbose_verbose_is_false():
    config(verbose=False)

    assert Configuration.verbose is False


def test_config_no_verbose_param_and_no_env_verbose_is_true():
    config()

    assert Configuration.verbose


def test_config_no_verbose_param_and_with_env_verbose_equals_to_false_verbose_is_false(monkeypatch):
    monkeypatch.setattr(os, "environ", {"LUMIGO_VERBOSE": "FALSE"})
    config()

    assert Configuration.verbose is False


@pytest.mark.parametrize(
    ("d", "keys", "result_value", "default"),
    [
        ({"k": ["a", "b"]}, ["k", 1], "b", None),  # Happy flow.
        ({"k": ["a"]}, ["k", 1], "default", "default"),  # List index out of range.
        ({"k": "a"}, ["b"], "default", "default"),  # Key doesn't exist.
        ({"k": "a"}, [1], "default", "default"),  # Wrong key type.
        ({"k": "a"}, ["k", 0, 1], "default", "default"),  # Wrong keys length.
    ],
)
def test_safe_get(d, keys, result_value, default):
    assert utils.safe_get(d, keys, default) == result_value


@pytest.mark.parametrize(
    ("regexes", "url", "expected"),
    [(["secret.*"], "lumigo.io", False), (["not-relevant", "secret.*"], "secret.aws.com", True)],
)
def test_should_scrub_domain(regexes, url, expected):
    Configuration.domains_scrubber = regexes
    assert should_scrub_domain(url) == expected
