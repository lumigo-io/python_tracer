![CircleCI](https://circleci.com/gh/lumigo-io/python_tracer/tree/master.svg?style=svg&circle-token=421fefe82bcad1c17c4116f154e25e32ebc90f2c)
![Version](https://badge.fury.io/py/lumigo-tracer.svg)
![codecov](https://codecov.io/gh/lumigo-io/python_tracer/branch/master/graph/badge.svg?token=6EgXIlefwG)

# How To Use
## With Lambda Layers:
* Use the latest ARN version [from these tables](https://github.com/lumigo-io/python_tracer/tree/master/layers)
## With Serverless framework:
* Install the [**serverless-lumigo-plugin**](https://github.com/lumigo-io/serverless-lumigo-plugin/blob/master/README.md)
## Manually
* install with `pip install lumigo_tracer` <br/> 
* import using `from lumigo_tracer import lumigo_tracer`
* wrap you lambda function using `@lumigo_tracer` or `@lumigo_tracer(token='XXX')`. As an example, your lambda should look like: 
```
@lumigo_tracer(token='XXX')
def my_lambda(event, context):
    print('I can finally troubleshoot!')
```

## Configuration
* You can turn on the debug logs by setting the environment variable `LUMIGO_DEBUG=true`
* You can prevent lumigo from sending keys that answer specific regexes by defining `LUMIGO_SECRET_MASKING_REGEX=["regex1", "regex2"]`. By default, we use the default regexes `[".*pass.*", ".*key.*"]`. All the regexes are case-insensitive.
* Similarly, you can prevent lumigo from sending the entire headers and body of specific domains using the environment variable `LUMIGO_DOMAINS_SCRUBBER=[".*secret.*"]` (give it a list which is a json parsable), or by specify the list of regexes with the key `domains_scrubber` in the tracer's decorator. By default, we will use `["secretsmanager\..*\.amazonaws\.com", "ssm\..*\.amazonaws\.com", "kms\..*\.amazonaws\.com"]`.
* In case of need, there is a kill switch, that stops all the interventions of lumigo immediately, without changing the code. Simply add an environment variable `LUMIGO_SWITCH_OFF=true`.

### Logging Programmatic Errors
You can use `report_error` function to write logs which will be visible in the platform.<br/>
Add `from lumigo_tracer import report_error`.<br/>

Then use `report_error("<msg>")` from anywhere in your lambda code.

### Step Functions
If this function is part of a step function, you can add the flag `step_function=True` or environment variable `LUMIGO_STEP_FUNCTION=True`, and we will track the states in the step function as a single transaction.
```
@lumigo_tracer(token='XXX', step_function=True)
def my_lambda(event, context):
    print('Step function visibility!')
```
Note: we will add the key `"_lumigo"` to the return value of the function. 

If you override the `"Parameters"` configuration, simply add `"_lumigo.$": "$._lumigo"`. <br/>
For example:
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
## Chalice
* In chalice, you should add the following lines to the your file:
```
from lumigo_tracer import LumigoChalice
...
app = Chalice(app_name='chalice')
app = LumigoChalice(app, token="XXX")
```

## Sentry/Raven Lambda Integration
Add our decorator beneath the Raven decorator
```
from lumigo_tracer import lumigo_tracer
...
@RavenLambdaWrapper()
@lumigo_tracer(token='XXX')
def lambda_handler (event, context):  return  {
 'statusCode' :  200,
 'body' : json.dumps( 'Hi!' ) }
```
# How To Contribute
Prepare your machine
----
* Create a virtualenv `virtualenv venv -p python3`
* Activate the virtualenv by running `. venv/bin/activate`
* Run `pip install -r requirements.txt` to install dependencies.
* `cd src` and `python setup.py develop`.
* If you use pycharm, make sure to change its virtualenv through the PyCharm -> Preferences -> Project -> Interpreter under the menu
* Run `pre-commit install` in your repository to install pre-commits hooks.

Test
----
* To run the unit tests, run `py.test` in the root folder.
* To deploy the services for the component tests, move to the root test directory and run `sls deploy`. This can be performed only once if the resources doesn't change.
* To run the component tests, run `py.test --all`.
