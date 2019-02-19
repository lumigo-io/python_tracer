import json
import subprocess
import boto3
import pytest


from lumigo_tracer.sync_http.sync_hook import lumigo_lambda


def events_by_mock(reporter_mock):
    # TODO - stop using mock. the reporter should send the events to some http server, and we should read from there.
    return reporter_mock.call_args[1]["msgs"]


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
    return f"arn:aws:sns:{region}:{account_id}:component_test"


@pytest.fixture
def lambda_resource():
    return "component-test"


@pytest.mark.slow
def test_dynamo_db(ddb_resource, reporter_mock, region):
    @lumigo_lambda
    def lambda_test_function():
        boto3.resource("dynamodb", region_name=region).Table(ddb_resource).put_item(
            Item={"key": "1"}
        )

    lambda_test_function()
    events = events_by_mock(reporter_mock)
    assert len(events) == 3
    assert events[1].get("url") == f"dynamodb.{region}.amazonaws.com"
    assert events[1].get("service") == "dynamodb"
    assert "id" in events[2]
    assert "ended" in events[0]


@pytest.mark.slow
def test_sns(sns_resource, reporter_mock, region):
    @lumigo_lambda
    def lambda_test_function():
        boto3.resource("sns").Topic(sns_resource).publish(Message=json.dumps({"test": "test"}))

    lambda_test_function()
    events = events_by_mock(reporter_mock)
    assert len(events) == 3
    assert events[1].get("url") == f"sns.{region}.amazonaws.com"
    assert events[1].get("service") == "sns"
    assert events[1].get("region") == region
    # assert events[2].get("messageId") is not None  # this is valid only when we read the body


@pytest.mark.slow
def test_lambda(lambda_resource, reporter_mock, region):
    @lumigo_lambda
    def lambda_test_function():
        boto3.client("lambda").invoke(
            FunctionName=lambda_resource, InvocationType="Event", Payload=b"null"
        )

    lambda_test_function()
    events = events_by_mock(reporter_mock)
    assert len(events) == 3
    assert events[1].get("url") == f"lambda.{region}.amazonaws.com"
    assert events[1].get("service") == "lambda"
    assert events[1].get("region") == region
