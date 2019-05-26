import json
import subprocess
import boto3
import pytest


from lumigo_tracer.sync_http.sync_hook import lumigo_tracer
from lumigo_tracer.spans_container import SpansContainer


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


@pytest.fixture
def kinesis_resource(region):
    return "component-test"


@pytest.fixture
def sqs_resource(region):
    account_id = boto3.client("sts").get_caller_identity().get("Account")
    return f"https://sqs.{region}.amazonaws.com/{account_id}/component-test"


@pytest.mark.slow
def test_dynamo_db(ddb_resource, region):
    @lumigo_tracer(token="123")
    def lambda_test_function():
        boto3.resource("dynamodb", region_name=region).Table(ddb_resource).put_item(
            Item={"key": "1"}
        )

    lambda_test_function()
    events = SpansContainer.get_span().events
    assert len(events) == 2
    assert events[1]["info"]["httpInfo"]["host"] == f"dynamodb.{region}.amazonaws.com"
    assert events[1]["info"]["resourceName"] == ddb_resource
    assert "ended" in events[0]


@pytest.mark.slow
def test_sns(sns_resource, region):
    @lumigo_tracer(token="123")
    def lambda_test_function():
        boto3.resource("sns").Topic(sns_resource).publish(Message=json.dumps({"test": "test"}))

    lambda_test_function()
    events = SpansContainer.get_span().events
    assert len(events) == 2
    assert events[1]["info"]["httpInfo"]["host"] == f"sns.{region}.amazonaws.com"
    assert events[1]["info"]["resourceName"] == sns_resource
    # assert events[2].get("messageId") is not None  # this is valid only when we read the body


@pytest.mark.slow
def test_lambda(lambda_resource, region):
    @lumigo_tracer(token="123")
    def lambda_test_function():
        boto3.client("lambda").invoke(
            FunctionName=lambda_resource, InvocationType="Event", Payload=b"null"
        )

    lambda_test_function()
    events = SpansContainer.get_span().events
    assert len(events) == 2
    assert events[1]["info"]["httpInfo"]["host"] == f"lambda.{region}.amazonaws.com"
    assert events[1].get("id").count("-") == 4


@pytest.mark.slow
def test_kinesis(kinesis_resource, region):
    @lumigo_tracer(token="123")
    def lambda_test_function():
        boto3.client("kinesis").put_record(
            StreamName=kinesis_resource, Data=b"my data", PartitionKey="1"
        )

    lambda_test_function()
    events = SpansContainer.get_span().events
    assert len(events) == 2
    assert events[1]["info"]["httpInfo"]["host"] == f"kinesis.{region}.amazonaws.com"
    assert events[1]["info"]["resourceName"] == kinesis_resource


@pytest.mark.slow
def test_sqs(sqs_resource, region):
    @lumigo_tracer(token="123")
    def lambda_test_function():
        boto3.client("sqs").send_message(QueueUrl=sqs_resource, MessageBody="myMessage")

    lambda_test_function()
    events = SpansContainer.get_span().events
    assert len(events) == 2
    assert events[1]["info"]["httpInfo"]["host"] == f"{region}.queue.amazonaws.com"
    assert events[1]["info"]["resourceName"] == sqs_resource
