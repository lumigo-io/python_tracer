#!/usr/bin/env bash
set -Eeo pipefail


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
    echo "${bold}Deploying ../src/test${normal}"
    pushd ./src/test > /dev/null
    echo "1"
    npm i > /dev/null 2>&1
    echo "2"
    sls deploy --force --env $env --region $aws_region
    echo "3"
}

deploy