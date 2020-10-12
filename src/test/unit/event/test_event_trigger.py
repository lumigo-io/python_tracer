import pytest

from lumigo_tracer.event.event_trigger import parse_triggered_by
from lumigo_tracer.lumigo_utils import Configuration


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
        (  # SQS example batch trigger
            {
                "Records": [
                    {
                        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:sqs-queue-name",
                        "eventSource": "aws:sqs",
                        "messageId": "1",
                    },
                    {
                        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:sqs-queue-name",
                        "eventSource": "aws:sqs",
                        "messageId": "2",
                    },
                ]
            },
            {
                "triggeredBy": "sqs",
                "arn": "arn:aws:sqs:us-east-1:123456789:sqs-queue-name",
                "messageIds": ["1", "2"],
            },
        ),
        (  # Step Function
            {
                "bla": "saart",
                "_lumigo": {"step_function_uid": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"},
            },
            {"triggeredBy": "stepFunction", "messageId": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"},
        ),
        (  # Inner Step Function
            {
                "bla": "saart",
                "inner": {"_lumigo": {"step_function_uid": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"}},
            },
            {"triggeredBy": "stepFunction", "messageId": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"},
        ),
        (  # Step Function from list
            [
                {
                    "bla": "saart",
                    "inner": {
                        "_lumigo": {"step_function_uid": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"}
                    },
                },
                {"something": "else"},
            ],
            {"triggeredBy": "stepFunction", "messageId": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"},
        ),
        (  # Step Function from inner list
            {
                "bla": "saart",
                "inner": [
                    {"_lumigo": {"step_function_uid": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"}},
                    {"something": "else"},
                ],
            },
            {"triggeredBy": "stepFunction", "messageId": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"},
        ),
        (  # Step Function - too deep
            {
                "bla": "saart",
                "a": {
                    "b": {
                        "c": {
                            "d": {
                                "_lumigo": {
                                    "step_function_uid": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"
                                }
                            }
                        }
                    }
                },
            },
            {"triggeredBy": "unknown"},
        ),
        (  # cloudwatch
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
        (  # unknown
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
        (  # cloudwatch
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
                "requestContext": {
                    "elb": {
                        "targetGroupArn": "arn:aws:elasticloadbalancing:us-east-1:195175520793:targetgroup/5a3a356429c37a7b4b104b399bab2a57/e7d3bec316fe42ea"
                    }
                },
                "httpMethod": "POST",
                "path": "/commands/bank/iou",
                "queryStringParameters": {},
                "headers": {
                    "accept": "application/json,*/*",
                    "accept-encoding": "gzip,deflate",
                    "cache-control": "max-age=259200",
                    "connection": "keep-alive",
                    "content-length": "355",
                    "content-type": "application/x-www-form-urlencoded",
                    "host": "lambd-loadb-bp68mp6nujg0-50156485.us-east-1.elb.amazonaws.com",
                },
            },
            {"triggeredBy": "unknown"},
        ),
        (  # API GW V2
            {
                "version": "2.0",
                "headers": {},
                "requestContext": {
                    "domainName": "r3pmxmplak.execute-api.us-east-2.amazonaws.com",
                    "domainPrefix": "r3pmxmplak",
                    "http": {
                        "method": "GET",
                        "path": "/default/nodejs-apig-function-1G3XMPLZXVXYI",
                    },
                    "requestId": "JKJaXmPLvHcESHA=",
                    "stage": "default",
                },
            },
            {
                "triggeredBy": "apigw",
                "httpMethod": "GET",
                "resource": "/default/nodejs-apig-function-1G3XMPLZXVXYI",
                "messageId": "JKJaXmPLvHcESHA=",
                "api": "r3pmxmplak.execute-api.us-east-2.amazonaws.com",
                "stage": "default",
            },
        ),
        ({"bla": "bla2"}, {"triggeredBy": "unknown"}),  # unknown trigger
        (None, None),
        (  # ddb - modify with keys
            {
                "Records": [
                    {
                        "eventSourceARN": "arn:aws:dynamodb:us-west-2:723663554526:table/abbbbb/stream/2020-05-25T12:04:49.788",
                        "eventSource": "aws:dynamodb",
                        "eventName": "MODIFY",
                        "dynamodb": {"ApproximateCreationDateTime": 1, "Keys": {"a": "b"}},
                    }
                ]
            },
            {
                "triggeredBy": "dynamodb",
                "messageIds": ["bd722b96a0bfdc0ef6115a2ee60b63f0"],
                "approxEventCreationTime": 1000,
                "arn": "arn:aws:dynamodb:us-west-2:723663554526:table/abbbbb/stream/2020-05-25T12:04:49.788",
            },
        ),
        (  # ddb - insert with NewImage
            {
                "Records": [
                    {
                        "eventSourceARN": "arn:aws:dynamodb:us-west-2:723663554526:table/abbbbb/stream/2020-05-25T12:04:49.788",
                        "eventSource": "aws:dynamodb",
                        "eventName": "INSERT",
                        "dynamodb": {"ApproximateCreationDateTime": 1, "NewImage": {"a": "b"}},
                    }
                ]
            },
            {
                "triggeredBy": "dynamodb",
                "messageIds": ["bd722b96a0bfdc0ef6115a2ee60b63f0"],
                "approxEventCreationTime": 1000,
                "arn": "arn:aws:dynamodb:us-west-2:723663554526:table/abbbbb/stream/2020-05-25T12:04:49.788",
            },
        ),
        (  # ddb - insert with only keys
            {
                "Records": [
                    {
                        "eventSourceARN": "arn:aws:dynamodb:us-west-2:723663554526:table/abbbbb/stream/2020-05-25T12:04:49.788",
                        "eventSource": "aws:dynamodb",
                        "eventName": "INSERT",
                        "dynamodb": {"ApproximateCreationDateTime": 1, "Keys": {"a": "b"}},
                    }
                ]
            },
            {
                "triggeredBy": "dynamodb",
                "messageIds": [],
                "approxEventCreationTime": 1000,
                "arn": "arn:aws:dynamodb:us-west-2:723663554526:table/abbbbb/stream/2020-05-25T12:04:49.788",
            },
        ),
    ],
)
def test_parse_triggered_by(event, output):
    Configuration.is_step_function = True
    assert parse_triggered_by(event) == output
