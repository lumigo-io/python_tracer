![Version](https://img.shields.io/badge/version-1.0.0-green.svg)
![CircleCI](https://circleci.com/gh/lumigo-io/python_tracer/tree/master.svg?style=svg&circle-token=421fefe82bcad1c17c4116f154e25e32ebc90f2c)
![codecov](https://codecov.io/gh/lumigo-io/python_tracer/branch/master/graph/badge.svg?token=6EgXIlefwG)

# Prepare your machine
* Create a virtualenv `virtualenv venv -p python3`
* Activate the virtualenv by running `. venv/bin/activate`
* Run `pip install -r requirements.txt` to install dependencies.
* If you use pycharm, make sure to change its virtualenv through the PyCharm -> Preferences -> Project -> Interpreter under the menu
* Run `pre-commit install` in your repository to install pre-commits hooks.

# Testing
* Run `pytest` in the root folder.
