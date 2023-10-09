#!/usr/bin/env bash
set -eo pipefail

pre-commit run -a
pushd src
py.test --all --cov=./lumigo_tracer --cov-config=.coveragerc

# TODO: We should have better tests for dependencies versions
echo "Testing sqlalchemy 1.3.16..."
python -m pip install sqlalchemy==1.3.16 > /dev/null
py.test -k sqlalchemy
popd