#!/usr/bin/env bash
set -eo pipefail

pre-commit run -a
cd src/test
python -m pytest --all
cd ../..