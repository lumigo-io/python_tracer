#!/usr/bin/env bash
set -eo pipefail

pre-commit run -a
pushd src
py.test --all --cov=./lumigo_tracer
popd