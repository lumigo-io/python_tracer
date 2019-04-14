![Version](https://img.shields.io/badge/version-1.0.20-green.svg)
![CircleCI](https://circleci.com/gh/lumigo-io/python_tracer/tree/master.svg?style=svg&circle-token=421fefe82bcad1c17c4116f154e25e32ebc90f2c)
![codecov](https://codecov.io/gh/lumigo-io/python_tracer/branch/master/graph/badge.svg?token=6EgXIlefwG)

# How To Use
* install with `pip install lumigo_tracer`
* import using `from lumigo_tracer import lumigo_tracer`
* wrap you lambda function using `@lumigo_tracer` or `@lumigo_tracer(token='XXX')`. As an example, your lambda should look like: 
```
@lumigo_tracer(token='XXX')
def my_lambda(event, context):
    print('I cant finally trubleshoot!')
```
* you can find more examples in the examples directory 
* In case of need, there is a kill switch, that stops all the interventions of lumigo immediately, without changing the code. Simply add an environment variable `LUMIGO_SWITCH_OFF=true`.
* You can turn on the debug logs by setting the environment variable `LUMIGO_DEBUG=true`
* You can change the timeout to send the trace information to the edge by setting `LUMIGO_EDGE_TIMEOUT=<seconds>`


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