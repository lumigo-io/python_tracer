import json

import pytest

from lumigo_tracer.event.event_trigger import parse_triggers
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
            [
                {
                    "extra": {
                        "httpMethod": "GET",
                        "api": "www.google.com",
                        "stage": "1",
                        "resource": "resource",
                    },
                    "triggeredBy": "apigw",
                    "fromMessageIds": ["123"],
                }
            ],
        ),
        (  # should be unknown
            {
                "resource": "/graphql",
                "path": "/graphql",
                "httpMethod": "POST",
                "body": '{"query":"query {\\n getSessionTemplatesForPlan: getSessionTemplatesForPlan(planId: \\"asggfds\\") {\\n sessionTemplateId: sessionTemplateId\\n name: name\\n }\\n}"}',
                "requestContext": {},
                "headers": {
                    "Authorization": "****",
                    "content-type": "application/json",
                    "Host": "aaaaaaaa.appsync-api.eu-west-1.amazonaws.com",
                    "origin": "https://app.sport.de",
                    "Referer": "https://app.sport.de/sessions/start/12345",
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.2 Safari/605.1.15",
                },
                "stageVariables": "null",
                "isBase64Encoded": "false",
            },
            [],
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
            [
                {
                    "extra": {
                        "arn": "arn:aws:sns:us-east-1:123456789:sns-topic-name",
                        "recordsNum": 1,
                    },
                    "triggeredBy": "sns",
                    "fromMessageIds": ["9cecb7e5-b11e-59fa-95c8-e28d3f64d6a8"],
                }
            ],
        ),
        (  # s3 example trigger
            {
                "Records": [
                    {
                        "s3": {"bucket": {"arn": "arn:aws:s3:::s3-bucket-name"}},
                        "awsRegion": "us-east-1",
                        "eventName": "ObjectCreated:Put",
                        "eventSource": "aws:s3",
                        "responseElements": {"x-amz-request-id": "11111111111111"},
                    }
                ]
            },
            [
                {
                    "extra": {
                        "arn": "arn:aws:s3:::s3-bucket-name",
                        "recordsNum": 1,
                    },
                    "triggeredBy": "s3",
                    "fromMessageIds": ["11111111111111"],
                }
            ],
        ),
        (  # kinesis example trigger
            {
                "Records": [
                    {
                        "eventSourceARN": "arn:aws:kinesis:us-east-1:123456789:stream/kinesis-stream-name",
                        "eventSource": "aws:kinesis",
                        "eventID": "shardId-000000000006:49590338271490256608559692538361571095921575989136588898",
                        "kinesis": {"sequenceNumber": "12"},
                    }
                ]
            },
            [
                {
                    "extra": {
                        "arn": "arn:aws:kinesis:us-east-1:123456789:stream/kinesis-stream-name",
                        "recordsNum": 1,
                        "shardId": "shardId-000000000006",
                    },
                    "triggeredBy": "kinesis",
                    "fromMessageIds": ["12"],
                }
            ],
        ),
        (  # kinesis example trigger - multiple records
            {
                "Records": [
                    {
                        "eventSourceARN": "arn:aws:kinesis:us-east-1:123456789:stream/kinesis-stream-name",
                        "eventSource": "aws:kinesis",
                        "eventID": "shardId-000000000006:49590338271490256608559692538361571095921575989136588898",
                        "kinesis": {"sequenceNumber": "12"},
                    },
                    {
                        "eventSourceARN": "arn:aws:kinesis:us-east-1:123456789:stream/kinesis-stream-name",
                        "eventSource": "aws:kinesis",
                        "eventID": "shardId-000000000006:49590338271490256608559692538361571095921575989136588898",
                        "kinesis": {"sequenceNumber": "34"},
                    },
                ]
            },
            [
                {
                    "extra": {
                        "arn": "arn:aws:kinesis:us-east-1:123456789:stream/kinesis-stream-name",
                        "recordsNum": 2,
                        "shardId": "shardId-000000000006",
                    },
                    "triggeredBy": "kinesis",
                    "fromMessageIds": ["12", "34"],
                }
            ],
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
            [
                {
                    "extra": {
                        "arn": "arn:aws:sqs:us-east-1:123456789:sqs-queue-name",
                        "recordsNum": 1,
                    },
                    "triggeredBy": "sqs",
                    "fromMessageIds": ["e97ff404-96ca-460e-8ff0-a46012e61826"],
                }
            ],
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
            [
                {
                    "extra": {
                        "arn": "arn:aws:sqs:us-east-1:123456789:sqs-queue-name",
                        "recordsNum": 2,
                    },
                    "triggeredBy": "sqs",
                    "fromMessageIds": ["1", "2"],
                }
            ],
        ),
        (  # SNS-SQS example trigger
            {
                "Records": [
                    {
                        "messageId": "f4ceb23d-2ae7-44d3-b171-df7ab2d10a81",
                        "receiptHandle": "BLABLA",
                        "body": '{\n  "Type" : "Notification",\n  "MessageId" : "2c78f253-4cd9-57bb-8bc3-a965e40a293e",\n  "TopicArn" : "arn:aws:sns:us-west-2:723663554526:tracer-test-saart-temp-Pttcj",\n  "Message" : "{}",\n  "Timestamp" : "2022-06-29T19:22:59.929Z",\n  "SignatureVersion" : "1",\n  "Signature" : "BLABLA",\n  "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-blablabla.pem",\n  "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:723663554526:tracer-test-saart-temp-Pttcj:blablabla"\n}',
                        "attributes": {
                            "ApproximateReceiveCount": "1",
                            "AWSTraceHeader": "Root=1-62bca693-5fa5fe5643dd0b8814c6524c;Parent=50e9a851030eeaee;Sampled=0",
                            "SentTimestamp": "1656530579970",
                            "SenderId": "saart",
                            "ApproximateFirstReceiveTimestamp": "1656530580976",
                        },
                        "messageAttributes": {},
                        "md5OfBody": "11111111111111",
                        "eventSource": "aws:sqs",
                        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:sqs-queue-name",
                        "awsRegion": "us-west-2",
                    }
                ]
            },
            [
                {
                    "extra": {
                        "arn": "arn:aws:sqs:us-east-1:123456789:sqs-queue-name",
                        "recordsNum": 1,
                    },
                    "fromMessageIds": [
                        "f4ceb23d-2ae7-44d3-b171-df7ab2d10a81",
                    ],
                    "triggeredBy": "sqs",
                },
                {
                    "extra": {
                        "arn": "arn:aws:sns:us-west-2:723663554526:tracer-test-saart-temp-Pttcj",
                        "recordsNum": 1,
                    },
                    "fromMessageIds": ["2c78f253-4cd9-57bb-8bc3-a965e40a293e"],
                    "triggeredBy": "sns",
                },
            ],
        ),
        (  # SQS that is *not* SNS-SQS (not SimpleNotificationService)
            {
                "Records": [
                    {
                        "messageId": "f4ceb23d-2ae7-44d3-b171-df7ab2d10a81",
                        "receiptHandle": "BLABLA",
                        "body": '{\n  "Type" : "Notification",\n  "MessageId" : "something else",\n  "TopicArn" : "arn:aws:sns:us-west-2:723663554526:tracer-test-saart-temp-Pttcj",\n  "Message" : "{}",\n  "Timestamp" : "2022-06-29T19:22:59.929Z",\n  "SignatureVersion" : "1",\n  "Signature" : "BLABLA",\n  "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/OtherNotificationService-blablabla.pem",\n  "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:723663554526:tracer-test-saart-temp-Pttcj:blablabla"\n}',
                        "attributes": {
                            "ApproximateReceiveCount": "1",
                            "AWSTraceHeader": "Root=1-62bca693-5fa5fe5643dd0b8814c6524c;Parent=50e9a851030eeaee;Sampled=0",
                            "SentTimestamp": "1656530579970",
                            "SenderId": "saart",
                            "ApproximateFirstReceiveTimestamp": "1656530580976",
                        },
                        "messageAttributes": {},
                        "md5OfBody": "11111111111111",
                        "eventSource": "aws:sqs",
                        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:sqs-queue-name",
                        "awsRegion": "us-west-2",
                    }
                ]
            },
            [
                {
                    "extra": {
                        "arn": "arn:aws:sqs:us-east-1:123456789:sqs-queue-name",
                        "recordsNum": 1,
                    },
                    "triggeredBy": "sqs",
                    "fromMessageIds": ["f4ceb23d-2ae7-44d3-b171-df7ab2d10a81"],
                }
            ],
        ),
        (  # Step Function
            {
                "bla": "saart",
                "_lumigo": {"step_function_uid": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"},
            },
            [
                {
                    "triggeredBy": "stepFunction",
                    "fromMessageIds": ["54589cfc-5ed8-4799-8fc0-5b45f6f225d1"],
                }
            ],
        ),
        (  # Inner Step Function
            {
                "bla": "saart",
                "inner": {"_lumigo": {"step_function_uid": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"}},
            },
            [
                {
                    "triggeredBy": "stepFunction",
                    "fromMessageIds": ["54589cfc-5ed8-4799-8fc0-5b45f6f225d1"],
                }
            ],
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
            [
                {
                    "triggeredBy": "stepFunction",
                    "fromMessageIds": ["54589cfc-5ed8-4799-8fc0-5b45f6f225d1"],
                }
            ],
        ),
        (  # Step Function from inner list
            {
                "bla": "saart",
                "inner": [
                    {"_lumigo": {"step_function_uid": "54589cfc-5ed8-4799-8fc0-5b45f6f225d1"}},
                    {"something": "else"},
                ],
            },
            [
                {
                    "triggeredBy": "stepFunction",
                    "fromMessageIds": ["54589cfc-5ed8-4799-8fc0-5b45f6f225d1"],
                }
            ],
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
            [],
        ),
        (  # EventBridge - happy flow
            {
                "version": "0",
                "id": "f0f73aaa-e64f-a550-5be2-850898090583",
                "detail-type": "string",
                "source": "source_lambda",
                "time": "2020-10-19T13:34:29Z",
                "region": "us-west-2",
                "resources": [],
                "detail": {"a": 0.024995371455989845},
            },
            [
                {
                    "triggeredBy": "eventBridge",
                    "fromMessageIds": ["f0f73aaa-e64f-a550-5be2-850898090583"],
                }
            ],
        ),
        (  # AppSync - happy flow
            {
                "context": {
                    "request": {
                        "headers": {
                            "x-amzn-trace-id": "Root=1-5fa161de-275509e254bf71cc48fd66d0",
                            "host": "oookuwqyrfhy7eexerofkmlbfm.appsync-api.eu-west-1.amazonaws.com",
                        }
                    }
                }
            },
            [
                {
                    "extra": {
                        "api": "oookuwqyrfhy7eexerofkmlbfm.appsync-api.eu-west-1.amazonaws.com",
                    },
                    "triggeredBy": "appsync",
                    "fromMessageIds": ["1-5fa161de-275509e254bf71cc48fd66d0"],
                }
            ],
        ),
        (  # AppSync - happy flow - different event struct
            {
                "request": {
                    "headers": {
                        "x-amzn-trace-id": "Root=1-5fa161de-275509e254bf71cc48fd66d0",
                        "host": "oookuwqyrfhy7eexerofkmlbfm.appsync-api.eu-west-1.amazonaws.com",
                    }
                }
            },
            [
                {
                    "extra": {
                        "api": "oookuwqyrfhy7eexerofkmlbfm.appsync-api.eu-west-1.amazonaws.com",
                    },
                    "triggeredBy": "appsync",
                    "fromMessageIds": ["1-5fa161de-275509e254bf71cc48fd66d0"],
                }
            ],
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
            [
                {
                    "extra": {
                        "resource": "ExampleRule",
                        "region": "us-east-1",
                        "detailType": "Scheduled Event",
                    },
                    "triggeredBy": "cloudwatch",
                }
            ],
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
            [],
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
            [
                {
                    "extra": {
                        "resource": "unknown",
                        "region": "us-east-1",
                        "detailType": "Scheduled Event",
                    },
                    "triggeredBy": "cloudwatch",
                }
            ],
        ),
        (  # old elb example trigger
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
            [
                {
                    "extra": {
                        "api": "lambd-loadb-bp68mp6nujg0-50156485.us-east-1.elb.amazonaws.com",
                        "httpMethod": "POST",
                    },
                    "triggeredBy": "load_balancer",
                }
            ],
        ),
        (  # new elb example trigger
            {
                "requestContext": {
                    "elb": {
                        "targetGroupArn": "arn:aws:elasticloadbalancing:region:123456789012:targetgroup/my-target-group/111"
                    }
                },
                "httpMethod": "GET",
                "path": "/",
                "queryStringParameters": {},
                "headers": {
                    "accept": "text/html,application/xhtml+xml",
                    "accept-language": "en-US,en;q=0.8",
                    "content-type": "text/plain",
                    "cookie": "cookies",
                    "host": "lambda-111-us-east-2.elb.amazonaws.com",
                    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6)",
                    "x-amzn-trace-id": "Root=1-111",
                    "x-forwarded-for": "111",
                    "x-forwarded-port": "111",
                    "x-forwarded-proto": "https",
                },
                "isBase64Encoded": False,
                "body": "request_body",
            },
            [
                {
                    "extra": {
                        "api": "lambda-111-us-east-2.elb.amazonaws.com",
                        "httpMethod": "GET",
                    },
                    "triggeredBy": "load_balancer",
                }
            ],
        ),
        (  # alb example trigger
            {
                "requestContext": {
                    "alb": {
                        "targetGroupArn": "arn:aws:elasticloadbalancing:region:123456789012:targetgroup/my-target-group/111"
                    }
                },
                "httpMethod": "GET",
                "path": "/",
                "queryStringParameters": {},
                "headers": {
                    "accept": "text/html,application/xhtml+xml",
                    "accept-language": "en-US,en;q=0.8",
                    "content-type": "text/plain",
                    "cookie": "cookies",
                    "host": "lambda-111-us-east-2.elb.amazonaws.com",
                    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6)",
                    "x-amzn-trace-id": "Root=1-111",
                    "x-forwarded-for": "111",
                    "x-forwarded-port": "111",
                    "x-forwarded-proto": "https",
                },
                "isBase64Encoded": False,
                "body": "request_body",
            },
            [
                {
                    "extra": {
                        "api": "lambda-111-us-east-2.elb.amazonaws.com",
                        "httpMethod": "GET",
                    },
                    "triggeredBy": "load_balancer",
                }
            ],
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
            [
                {
                    "extra": {
                        "httpMethod": "GET",
                        "resource": "/default/nodejs-apig-function-1G3XMPLZXVXYI",
                        "api": "r3pmxmplak.execute-api.us-east-2.amazonaws.com",
                        "stage": "default",
                    },
                    "triggeredBy": "apigw",
                    "fromMessageIds": ["JKJaXmPLvHcESHA="],
                }
            ],
        ),
        ({"bla": "bla2"}, []),  # unknown trigger
        (None, []),
        (  # ddb - modify with keys
            {
                "Records": [
                    {
                        "eventSourceARN": "arn:aws:dynamodb:us-west-2:723663554526:table/abbbbb/stream/2020-05-25T12:04:49.788",
                        "eventSource": "aws:dynamodb",
                        "eventName": "MODIFY",
                        "dynamodb": {
                            "ApproximateCreationDateTime": 1,
                            "Keys": {"a": "b"},
                            "SizeBytes": 1,
                        },
                    }
                ]
            },
            [
                {
                    "extra": {
                        "approxEventCreationTime": 1000,
                        "arn": "arn:aws:dynamodb:us-west-2:723663554526:table/abbbbb/stream/2020-05-25T12:04:49.788",
                        "recordsNum": 1,
                        "totalSizeBytes": 1,
                    },
                    "triggeredBy": "dynamodb",
                    "fromMessageIds": ["bd722b96a0bfdc0ef6115a2ee60b63f0"],
                }
            ],
        ),
        (  # ddb - insert with NewImage
            {
                "Records": [
                    {
                        "eventSourceARN": "arn:aws:dynamodb:us-west-2:723663554526:table/abbbbb/stream/2020-05-25T12:04:49.788",
                        "eventSource": "aws:dynamodb",
                        "eventName": "INSERT",
                        "dynamodb": {
                            "ApproximateCreationDateTime": 1,
                            "NewImage": {"a": "b"},
                            "SizeBytes": 1,
                        },
                    }
                ]
            },
            [
                {
                    "extra": {
                        "approxEventCreationTime": 1000,
                        "arn": "arn:aws:dynamodb:us-west-2:723663554526:table/abbbbb/stream/2020-05-25T12:04:49.788",
                        "recordsNum": 1,
                        "totalSizeBytes": 1,
                    },
                    "triggeredBy": "dynamodb",
                    "fromMessageIds": ["bd722b96a0bfdc0ef6115a2ee60b63f0"],
                }
            ],
        ),
        (  # ddb - insert with only keys
            {
                "Records": [
                    {
                        "eventSourceARN": "arn:aws:dynamodb:us-west-2:723663554526:table/abbbbb/stream/2020-05-25T12:04:49.788",
                        "eventSource": "aws:dynamodb",
                        "eventName": "INSERT",
                        "dynamodb": {
                            "ApproximateCreationDateTime": 1,
                            "Keys": {"a": "b"},
                            "SizeBytes": 1,
                        },
                    }
                ]
            },
            [
                {
                    "extra": {
                        "approxEventCreationTime": 1000,
                        "arn": "arn:aws:dynamodb:us-west-2:723663554526:table/abbbbb/stream/2020-05-25T12:04:49.788",
                        "recordsNum": 1,
                        "totalSizeBytes": 1,
                    },
                    "triggeredBy": "dynamodb",
                }
            ],
        ),
    ],
)
def test_parse_triggered_by(event, output):
    Configuration.is_step_function = True
    triggers = json.loads(json.dumps(parse_triggers(event)))
    assert len({t.pop("id") for t in triggers}) == len(output)
    [t.pop("targetId") for t in triggers]
    assert triggers == output


def test_recursive_trigger_by_linking():
    sns_to_sqs_event = {
        "Records": [
            {
                "messageId": "sqs-1",
                "receiptHandle": "BLABLA",
                "body": '{\n  "Type" : "Notification",\n  "MessageId" : "sns-1",\n  "TopicArn" : "arn:aws:sns:us-west-2:723663554526:tracer-test-saart-temp-Pttcj",\n  "Message" : "{}",\n  "Timestamp" : "2022-06-29T19:22:59.929Z",\n  "SignatureVersion" : "1",\n  "Signature" : "BLABLA",\n  "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-blablabla.pem",\n  "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:723663554526:tracer-test-saart-temp-Pttcj:blablabla"\n}',
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "AWSTraceHeader": "Root=1-111111-111111111;Parent=222222;Sampled=0",
                    "SentTimestamp": "1656530579970",
                    "SenderId": "saart",
                    "ApproximateFirstReceiveTimestamp": "1656530580976",
                },
                "messageAttributes": {},
                "md5OfBody": "11111111111111",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:sqs-queue-name",
                "awsRegion": "us-west-2",
            },
            {
                "messageId": "sqs-2",
                "receiptHandle": "BLABLA",
                "body": '{\n  "Type" : "Notification",\n  "MessageId" : "sns-2",\n  "TopicArn" : "arn:aws:sns:us-west-2:723663554526:tracer-test-saart-temp-Pttcj",\n  "Message" : "{}",\n  "Timestamp" : "2022-06-29T19:22:59.929Z",\n  "SignatureVersion" : "1",\n  "Signature" : "BLABLA",\n  "SigningCertURL" : "https://sns.us-west-2.amazonaws.com/SimpleNotificationService-blablabla.pem",\n  "UnsubscribeURL" : "https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:723663554526:tracer-test-saart-temp-Pttcj:blablabla"\n}',
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "AWSTraceHeader": "Root=1-111111-111111111;Parent=222222;Sampled=0",
                    "SentTimestamp": "1656530579970",
                    "SenderId": "saart",
                    "ApproximateFirstReceiveTimestamp": "1656530580976",
                },
                "messageAttributes": {},
                "md5OfBody": "11111111111111",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:sqs-queue-name",
                "awsRegion": "us-west-2",
            },
        ]
    }
    triggers = parse_triggers(sns_to_sqs_event)
    assert len(triggers) == 3
    sqs_trigger = next(t for t in triggers if t["targetId"] is None)
    sns_triggers = [t for t in triggers if t["targetId"] is not None]
    assert len(sns_triggers) == 2
    assert all(sqs_trigger["id"] == sns_trigger["targetId"] for sns_trigger in sns_triggers)

    assert set(sqs_trigger["fromMessageIds"]) == {"sqs-1", "sqs-2"}
    assert set(sns_trigger["fromMessageIds"][0] for sns_trigger in sns_triggers) == {
        "sns-1",
        "sns-2",
    }
