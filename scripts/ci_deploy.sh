#!/usr/bin/env bash
set -Eeo pipefail
# A script being used by circle ci. Should not be ran locally.

# Always delete the stack
function _finally {
    cd ~/lumigo-api
    ./scripts/remove.sh --stage integrationIt --region ${region}
    cd ~/repo
    ./scripts/remove.sh --stage integrationIt --region ${region}
}

trap _finally EXIT

export USER=integration

region=us-east-1
pushd ..
git clone git@github.com:lumigo-io/lumigo-api.git
popd

pushd ../lumigo-api
./scripts/deploy.sh --encrypted-file credentials_integration.enc --stage integrationIt --region ${region}
popd

pushd ./python_tracer
python setup.py install
popd

./scripts/deploy.sh --stage integrationIt --region ${region} --stage-backend integrationIt --force