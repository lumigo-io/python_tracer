import re
from typing import List, Type

from .event_trigger_base import EventTriggerParser
from .step_function_parser import StepFunctionEventTriggerParser
from .apigw_parser import ApiGatewayEventTriggerParser
from .load_balancer_parser import LoadBalancerEventTriggerParser
from .sns_parser import SnsEventTriggerParser
from .s3_parser import S3EventTriggerParser
from .sqs_parser import SqsEventTriggerParser
from .kinesis_parser import KinesisEventTriggerParser
from .dynamodb_parser import DynamoDBEventTriggerParser
from .cloudwatch_parser import CloudwatchEventTriggerParser
from .eventbridge_parser import EventbridgeEventTriggerParser
from .appsync_parser import AppsyncEventTriggerParser


EVENT_TRIGGER_PARSERS: List[Type[EventTriggerParser]] = [
    StepFunctionEventTriggerParser,
    ApiGatewayEventTriggerParser,
    LoadBalancerEventTriggerParser,
    SnsEventTriggerParser,
    S3EventTriggerParser,
    SqsEventTriggerParser,
    KinesisEventTriggerParser,
    DynamoDBEventTriggerParser,
    CloudwatchEventTriggerParser,
    EventbridgeEventTriggerParser,
    AppsyncEventTriggerParser,
]
INNER_MESSAGES_MAGIC_PATTERN = re.compile(
    r"("
    + "|".join(
        parser.MAGIC_IDENTIFIER for parser in EVENT_TRIGGER_PARSERS if parser.MAGIC_IDENTIFIER
    )
    + ")"
)
