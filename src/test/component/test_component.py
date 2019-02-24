import json
import subprocess
import boto3
import pytest


from lumigo_tracer.sync_http.sync_hook import lumigo_lambda
from lumigo_tracer.span import Span


@pytest.fixture(scope="session", autouse=True)
def serverless_yaml():
    subprocess.check_output(["sls", "deploy"])


@pytest.fixture
def region():
    return boto3.session.Session().region_name


@pytest.fixture
def ddb_resource(region):
    return "component-test"


@pytest.fixture
def sns_resource(region):
    account_id = boto3.client("sts").get_caller_identity().get("Account")
    region = boto3.session.Session().region_name
    return f"arn:aws:sns:{region}:{account_id}:component-test"


@pytest.fixture
def lambda_resource():
    return "component-test"


@pytest.mark.slow
def test_dynamo_db(ddb_resource, region):
    @lumigo_lambda
    def lambda_test_function():
        boto3.resource("dynamodb", region_name=region).Table(ddb_resource).put_item(
            Item={"key": "1"}
        )

    lambda_test_function()
    events = Span.get_span().events
    assert len(events) == 2
    assert events[1]["info"]["httpInfo"]["host"] == f"dynamodb.{region}.amazonaws.com"
    assert events[1].get("service") == "dynamodb"
    assert events[1].get("name") == ddb_resource
    assert "ended" in events[0]


@pytest.mark.slow
def test_sns(sns_resource, region):
    @lumigo_lambda
    def lambda_test_function():
        boto3.resource("sns").Topic(sns_resource).publish(Message=json.dumps({"test": "test"}))

    lambda_test_function()
    events = Span.get_span().events
    assert len(events) == 3
    assert events[2]["info"]["httpInfo"]["host"] == f"sns.{region}.amazonaws.com"
    assert events[2].get("service") == "sns"
    # assert events[2].get("messageId") is not None  # this is valid only when we read the body


@pytest.mark.slow
def test_lambda(lambda_resource, region):
    @lumigo_lambda
    def lambda_test_function():
        boto3.client("lambda").invoke(
            FunctionName=lambda_resource, InvocationType="Event", Payload=b"null"
        )

    lambda_test_function()
    events = Span.get_span().events
    assert len(events) == 2
    assert events[1]["info"]["httpInfo"]["host"] == f"lambda.{region}.amazonaws.com"
    assert events[1].get("service") == "lambda"
    assert events[1].get("id").count("-") == 4
