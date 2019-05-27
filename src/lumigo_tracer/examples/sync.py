import json

import boto3
import urllib.request
from lumigo_tracer import lumigo_tracer


@lumigo_tracer(token="123")
def my_lambda(arg1, arg2):
    boto3.resource("dynamodb", region_name="us-east-2").Table("test").put_item(
        Item={"a": 1, "key": 2}
    )

    boto3.client("sns").publish(
        TargetArn="arn:aws:sns:us-east-2:account_id:test", Message=json.dumps({"test": "test"})
    )

    urllib.request.urlopen("http://www.google.com")
