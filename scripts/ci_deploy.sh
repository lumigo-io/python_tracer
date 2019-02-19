#!/usr/bin/env bash
set -Eeo pipefail

# This function deploys a repo which is given as a parameter
# Example for an input: "tracing-ingestion-edge"
function checkout_and_deploy() {
    pushd ~/ > /dev/null
    git clone git@github.com:lumigo-io/${1}.git
    cd ~/${1}
    ./scripts/deploy.sh --region ${aws_region} --env int-${branch_name}-${user_name}
    popd > /dev/null
}

echo ".____                  .__                  .__        ";
echo "|    |    __ __  _____ |__| ____   ____     |__| ____  ";
echo "|    |   |  |  \/     \|  |/ ___\ /  _ \    |  |/  _ \ ";
echo "|    |___|  |  /  Y Y  \  / /_/  >  <_> )   |  (  <_> )";
echo "|_______ \____/|__|_|  /__\___  / \____/ /\ |__|\____/ ";
echo "        \/           \/  /_____/         \/            ";
echo
echo "Deploy to integration environment"

branch_name=$(echo ${CIRCLE_BRANCH} | cut -c1-8 | awk '{print tolower($0)}')
user_name=$(echo ${CIRCLE_USERNAME} | cut -c1-3 | awk '{print tolower($0)}')
aws_region=us-west-2
# For CircleCI
echo "export AWS_DEFAULT_REGION=${aws_region}" >> $BASH_ENV
if [[ "$branch_name" == "master" ]]; then
    branch_name=${branch_name}-$(echo ${CIRCLE_SHA1}|cut -c1-4)
fi
echo "export USER=int-${branch_name}-${user_name}" >> $BASH_ENV
echo "Branch: $branch_name"
echo "User: $user_name"

function deploy() {
    directory = "./src/test"
    echo "${bold}Deploying ${directory}${normal}"
    pushd $directory > /dev/null
    npm i > /dev/null 2>&1
    sls deploy --force --env $env --region $region
}

deploy()
