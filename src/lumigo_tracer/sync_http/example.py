import json

from lumigo_tracer.sync_http.sync_hook import lumigo_lambda
import boto3
import urllib.request


@lumigo_lambda
def my_lambda(arg1, arg2):
    boto3.resource("dynamodb", region_name="us-east-2").Table("test").put_item(
        Item={"a": 1, "key": 2}
    )

    boto3.client("sns").publish(
        TargetArn="arn:aws:sns:us-east-2:723663554526:test", Message=json.dumps({"test": "test"})
    )

    urllib.request.urlopen("http://www.google.com")


if __name__ == "__main__":
    my_lambda(1, 2)
