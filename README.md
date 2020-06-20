![CircleCI](https://circleci.com/gh/lumigo-io/python_tracer/tree/master.svg?style=svg&circle-token=421fefe82bcad1c17c4116f154e25e32ebc90f2c)
![Version](https://badge.fury.io/py/lumigo-tracer.svg)
![codecov](https://codecov.io/gh/lumigo-io/python_tracer/branch/master/graph/badge.svg?token=6EgXIlefwG)

This is the Python version of Lumigo's tracer library.

Supported Runtimes: 3.6, 3.7, 3.8

# Using the tracer
There are three ways to use the tracer with your AWS Lambda functions: using our Lambda Layer, using the Serverless framework, or instrumenting your functions manually.

## Lambda layers
Using our Lambda layer lets you automatically instrument your Lambda functions. Simply use the latest ARN [from these tables](https://github.com/lumigo-io/python_tracer/tree/master/layers)

## Serverless framework
When working with Serverless framework, Lumigo provides the [**serverless-lumigo-plugin**](https://github.com/lumigo-io/serverless-lumigo-plugin/blob/master/README.md) for integration. This plugin handles the complexity of integrating the Lumigo tracer with your serverless function.

## Manual
Manual installation of the Lumigo tracer package requires the following steps:

* Install the package: `pip install lumigo_tracer` 
* Import the package in your Lambda code: `from lumigo_tracer import lumigo_tracer`
* Wrap your function with the `@lumigo_tracer` or `@lumigo_tracer(token='XXX')` attributes.

Once you've configured the package and added the tracer attribute, your code should resemble the following:

```python
@lumigo_tracer(token='XXX')
def my_lambda(event, context):
    print('I can finally troubleshoot!')
```

## Configuration
There are several configuration environment variables you can adjust to change how the package behaves:

* `LUMIGO_DEBUG=true` - This environment variable controls debug logging. Set it to `true` to enable debug logging output
* `LUMIGO_SECRET_MASKING_REGEX=["regex1", "regex2"]` - This is a JSON-formatted list of regular expressions that are applied to keys sent by Lumigo. If a key matches one of these regexes, it is not sent. All regular expressions are case-insensitive. The default list of regexes checked is  `[".*pass.*", ".*key.*"]`. All the regexes are case-insensitive.
* `LUMIGO_DOMAINS_SCRUBBER=[".*secret.*"]` - This is a JSON-formatted list of regular expressions that can be used to prevent Lumigo from sending the entire headers and body of requests to specific domains. This can also be configured in the function decorator `@lumigo_tracer` using the parameter name `domains_scrubber`. The default list of regexes is `["secretsmanager\..*\.amazonaws\.com", "ssm\..*\.amazonaws\.com", "kms\..*\.amazonaws\.com"]`.
* `LUMIGO_SWITCH_OFF=true` - This is a kill switch that stops all Lumigo-driven behavior when flipped, without needing to change the underlying code. Simply add this environment variable to your function's configuration, and the tracer will be disabled.

### Logging Programmatic Errors
Lumigo provides the `report_error` function, which you can use to publish error logs that are visible to the entire platform. To log programmatic errors:

* Import the `report_error` function with the following code: `from lumigo_tracer import report_error`
* Use the `report_error` function with the message you wish to send: `report_error("your-message-here")`

### Adding Execution Tags
You can use `add_execution_tag` function to add an execution_tag with a dynamic value. These execution tags are searchable within the Lumigo platform, and can allow you to easily classify your function's operations in response to events as they occur. To use execution tags:

* Import the `add_execution_tag` function: `from lumigo_tracer import add_execution_tag`
* Call the function anywhere in your lambda code: `add_execution_tag("your-key", "your-value)`

Please note the following limitations:
* The maximum number of tags is 50.
* Key and value length must be between 1 and 50.


### Step Functions
When working with step functions, Lumigo gives you the capability to trace all of the states in your step function from within a single transaction. You can enable this functionality in one of two ways:

* Pass `step_function=True` to the `@lumigo_tracer` attribute
* Set the `LUMIGO_STEP_FUNCTION` environment variable to `True` in your function configuration.

Here's an example of enabling the step_function tracing in a function attribute:

```
@lumigo_tracer(token='XXX', step_function=True)
def my_lambda(event, context):
    print('Step function visibility!')
```

**Note**: With step function tracing active, the package will add the `_lumigo` key to the return value of your functions. If you have overridden the default `Parameters` configuration in your function, you'll need to add the following to accommodate the lumigo step tracer:

`"_lumigo.$": "$._lumigo"`

Below is an example state configuration with a modified `parameters` section:

```
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


# Frameworks

In addition to native code integration, Lumigo also provides tools for integrating with popular Python frameworks.

## Chalice

To work with the `lumigo_tracer` in a Chalice-driven function, perform the following:
* Import the `LumigoChalice` tracer: `from lumigo_tracer import LumigoChalice`
* Encapsulate your Chalice app within the LumigoChalice wrapper:

```python
app = Chalice(app_name='chalice')
app = LumigoChalice(app, token="XXX")
```

## Sentry/Raven Lambda Integration
To integrate the `lumigo_tracer` with Raven, perform the following:

* Include the ` lumigo_tracer` attribute in your code: `from lumigo_tracer import lumigo_tracer`
* Include the `@lumigo_tracer` decorator **beneath** the Raven decorator:

```python
@RavenLambdaWrapper()
@lumigo_tracer(token='XXX')
def lambda_handler (event, context):  return  {
 'statusCode' :  200,
 'body' : json.dumps( 'Hi!' ) }
```

# Contributing

Contributions to this project are welcome from all! Below are a couple pointers on how to prepare your machine, as well as some information on testing.

## Preparing your machine
Getting your machine ready to develop against the package is a straightforward process:

1. Clone this repository, and open a CLI in the cloned directory
1. Create a virtual environment for the project `virtualenv venv -p python3`
1. Activate the virtualenv: `. venv/bin/activate`
1. Install dependencies: `pip install -r requirements.txt`
1. Navigate to the source directory: `cd src` and 
1. Run the setup script: `python setup.py develop`.
1. Run `pre-commit install` in your repository to install pre-commit hooks

**Note**: If you are using pycharm, ensure that you set it to use the virtualenv virtual environment manager. This is available in the menu under PyCharm -> Preferences -> Project -> Interpreter


## Running the test suite
We've provided an easy way to run the unit test suite:

* To run all unit tests, simply run `py.test` in the root folder.
* To deploy services for component tests, run `sls deploy` from the root test directory. This only needs to take place when the resources change.
* To run component tests, add the `--all` flag: `py.test --all`
