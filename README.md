![Version](https://img.shields.io/badge/version-1.0.0-green.svg)
![CircleCI](https://circleci.com/gh/lumigo-io/python_tracer/tree/master.svg?style=svg&circle-token=a8020c3a3ad0c3f936f28296fd90f70b1e3a66d3)
![codecov](https://codecov.io/gh/lumigo-io/python_tracer/branch/master/graph/badge.svg?token=4vcFZFlJOm)


# Prepare your machine
* Create a virtualenv `virtualenv venv -p python3`
* Activate the virtualenv by running `. venv/bin/activate`
* Run `pip install -r requirements.txt` to install dependencies.
* If you use pycharm, make sure to change its virtualenv through the PyCharm -> Preferences -> Project -> Interpreter under the menu
* Run `pre-commit install` in your repository to install pre-commits hooks.

# Testing
* Run `pytest` in the root folder.
