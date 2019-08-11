#!/usr/bin/env bash
set -eo pipefail

pre-commit run -a
pushd src
python -m pytest --cov=./lumigo_tracer
popd