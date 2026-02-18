[![CircleCI](https://dl.circleci.com/status-badge/img/gh/lumigo-io/python_tracer/tree/master.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/lumigo-io/python_tracer/tree/master)
![Version](https://badge.fury.io/py/lumigo-tracer.svg)
![codecov](https://codecov.io/gh/lumigo-io/python_tracer/branch/master/graph/badge.svg?token=6EgXIlefwG)

This is lumigo/python_tracer, Lumigo's Python agent for distributed tracing and performance monitoring.

Supported Python Runtimes: 3.6, 3.7, 3.8, 3.9, 3.10, 3.11, 3.12, 3.13 and 3.14

# Usage

The package allows you to pursue automated metric gathering through Lambda Layers, automated metric gathering and instrumentation through the Serverless framework, or manual metric creation and implementation.

## With Lambda layers

* When configuring your Lambda functions, include the appropriate Lambda Layer ARN [from these tables](https://github.com/lumigo-io/python_tracer/tree/master/layers)

*Note* - Lambda Layers are an optional feature. If you decide to use this capability, the list of Lambda layers available is available [here.](https://github.com/lumigo-io/python_tracer/tree/master/layers).

Learn more in our [documentation on auto-instrumentation](https://docs.lumigo.io/docs/auto-instrumentation).

## With Serverless framework

* To configure the Serverless Framework to work with Lumigo, simply install our plugin: [**serverless-lumigo-plugin**](https://github.com/lumigo-io/serverless-lumigo-plugin/blob/master/README.md)

## Manually

To manually configure Lumigo in your Lambda functions:

* Install the package:

```bash
pip install lumigo_tracer
```

* Import the package in your Lambda code:

```python
`from lumigo_tracer import lumigo_tracer`
```

* Next, wrap your `handler` in Lumigo's `trace` function (note: replace `YOUR-TOKEN-HERE` with your Lumigo API token):

```python
@lumigo_tracer(token='YOUR-TOKEN-HERE')
def my_lambda(event, context):
    print('I can finally troubleshoot!')
```

* Your function is now fully instrumented

## Configuration

`@lumigo/python_tracer` offers several different configuration options. Pass these to the Lambda function as environment variables:

* `LUMIGO_DEBUG=TRUE` - Enables debug logging
* `LUMIGO_SECRET_MASKING_REGEX=["regex1", "regex2"]` - Prevents Lumigo from sending keys that match the supplied regular expressions. All regular expressions are case-insensitive. By default, Lumigo applies the following regular expressions: `[".*pass.*", ".*key.*", ".*secret.*", ".*credential.*", ".*passphrase.*"]`.
  * We support more granular masking using the following parameters. If not given, the above configuration is the fallback: `LUMIGO_SECRET_MASKING_REGEX_HTTP_REQUEST_BODIES`, `LUMIGO_SECRET_MASKING_REGEX_HTTP_REQUEST_HEADERS`, `LUMIGO_SECRET_MASKING_REGEX_HTTP_RESPONSE_BODIES`, `LUMIGO_SECRET_MASKING_REGEX_HTTP_RESPONSE_HEADERS`, `LUMIGO_SECRET_MASKING_REGEX_HTTP_QUERY_PARAMS`, `LUMIGO_SECRET_MASKING_REGEX_ENVIRONMENT`.
* `LUMIGO_DOMAINS_SCRUBBER=[".*secret.*"]` - Prevents Lumigo from collecting both request and response details from a list of domains. This accepts a comma-separated list of regular expressions that is JSON-formatted. By default, the tracer uses `["secretsmanager\..*\.amazonaws\.com", "ssm\..*\.amazonaws\.com", "kms\..*\.amazonaws\.com"]`. **Note** - These defaults are overridden when you define a different list of regular expressions.
* `LUMIGO_PROPAGATE_W3C=TRUE` - Add W3C TraceContext headers to outgoing HTTP requests. This enables uninterrupted transactions with applications traced with OpenTelemetry.
* `LUMIGO_SWITCH_OFF=TRUE` - In the event a critical issue arises, this turns off all actions that Lumigo takes in response to your code. This happens without a deployment, and is picked up on the next function run once the environment variable is present.

### Step Functions

If your function is part of a set of step functions, you can add the flag `step_function: true` to the Lumigo tracer import. Alternatively, you can configure the step function using an environment variable `LUMIGO_STEP_FUNCTION=True`. When this is active, Lumigo tracks all states in the step function in a single transaction, easing debugging and observability.

```python
@lumigo_tracer(token='XXX', step_function=True)
def my_lambda(event, context):
    print('Step function visibility!')
```

Note: the tracer adds the key `"_lumigo"` to the return value of the function.

If you override the `"Parameters"` configuration, add `"_lumigo.$": "$._lumigo"` to ensure this value is still present.

Below is an example configuration for a Lambda function that is part of a step function that has overridden its parameters:

```json
"States": {
    "state1": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-west-2:ACCOUNT:function:FUNCTION_NAME",
      "Parameters": {
          "Changed": "parameters",
          "_lumigo.$": "$._lumigo"
        },
      "Next": "state2"
    },
    "state2": {
      "Type": "pass",
      "End": true
    }
}
```

### Logging Programmatic Errors

Lumigo provides the `report_error` function, which you can use to publish error logs that are visible to the entire platform. To log programmatic errors:

* Import the `report_error` function with the following code: `from lumigo_tracer import report_error`
* Use the `report_error` function with the message you wish to send: `report_error("your-message-here")`

### Adding Execution Tags

You can add execution tags to a function with dynamic values using the parameter `add_execution_tag`.

These tags will be searchable from within the Lumigo platform.

#### Limitations

* Up to 50 execution tags
* Each tag key length can have 50 characters at most.
* Each tag value length can have 70 characters at most.

# Contributing

Contributions to this project are welcome from all! Below are a couple pointers on how to prepare your machine, as well as some information on testing.

## Preparing your machine

Getting your machine ready to develop against the package is a straightforward process:

1. Clone this repository, and open a CLI in the cloned directory
1. Create a virtual environment for the project `virtualenv venv -p python3`
1. Activate the virtualenv: `. venv/bin/activate`
1. Install dependencies: `pip install -r requirements.txt`
1. Run the setup script: `python setup.py develop`.
1. Run `pre-commit install` in your repository to install pre-commit hooks

**Note**: If you are using pycharm, ensure that you set it to use the virtualenv virtual environment manager. This is available in the menu under PyCharm -> Preferences -> Project -> Interpreter

## Running the test suite

We've provided an easy way to run the unit test suite:

* To run all unit tests, simply run `py.test` in the root folder.
* To deploy services for component tests, run `sls deploy` from the root test directory. This only needs to take place when the resources change.
* To run component tests, add the `--all` flag: `py.test --all`

