#!/usr/bin/env bash
set -Eeo pipefail

setup_git() {
    git config --global user.email "no-reply@build.com"
    git config --global user.name "CircleCI"
    git checkout master
    # Avoid npm version failure
    git stash
}

push_tags() {
    git push origin master --tags
}

echo ".____                  .__                  .__        ";
echo "|    |    __ __  _____ |__| ____   ____     |__| ____  ";
echo "|    |   |  |  \/     \|  |/ ___\ /  _ \    |  |/  _ \ ";
echo "|    |___|  |  /  Y Y  \  / /_/  >  <_> )   |  (  <_> )";
echo "|_______ \____/|__|_|  /__\___  / \____/ /\ |__|\____/ ";
echo "        \/           \/  /_____/         \/            ";
echo
echo "Deploy lumigo-logger to pypi server"

setup_git
echo "Getting latest changes from git"
changes=$(git log $(git describe --tags --abbrev=0)..HEAD --oneline)

sudo pip install --upgrade bumpversion
bumpversion patch --message "{current_version} → {new_version}. Changes: ${changes}"

echo "Uploading to gemfury"
echo "Setup"
pushd ./src > /dev/null
python setup.py sdist

echo "Upload"
upload_file=$(ls ./dist/*.gz)
curl -F package=@${upload_file} https://${FURY_AUTH}@push.fury.io/lumigo/
popd > /dev/null 2>&1

echo "Create Layer"
cd src
mkdir python
cp -R lumigo_tracer.egg-info python/
cp -R lumigo_tracer python/
cp -R VERSION python/
zip -r lumigo_tracer.zip python

regions=("ap-northeast-1" "ap-northeast-2" "ap-south-1" "ap-southeast-1" "ap-southeast-2" "ca-central-1" "eu-central-1" "eu-north-1" "eu-west-1" "eu-west-2" "eu-west-3" "sa-east-1" "us-east-1" "us-east-2" "us-west-1" "us-west-2")
for region in "${regions[@]}"; do
    version=$(aws lambda publish-layer-version --layer-name lumigo-python-tracer --description "Serverless Troubleshooting Make Simple" --license-info "Apache License Version 2.0" --zip-file fileb://lumigo_tracer.zip --compatible-runtimes python3.6 python3.7 --region ${region}| jq -r '.Version')
    aws lambda add-layer-version-permission --layer-name lumigo-python-tracer --statement-id engineering-org --principal "*" --action lambda:GetLayerVersion --version-number ${version} --region ${region}
    echo "published version ${version} to region ${region}"
done

echo "Create release tag"
push_tags

echo "Done"
