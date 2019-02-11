#!/usr/bin/env bash
set -eo pipefail

pre-commit run -a
python -m pytest src/test --all
