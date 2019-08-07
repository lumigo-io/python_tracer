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
enc_location=../common-resources/encrypted_files/credentials_production.enc
if [[ ! -f ${enc_location} ]]
then
    echo "$enc_location not found"
    exit 1
fi
echo "Creating new credential files"
mkdir -p ~/.aws
echo ${KEY} | gpg --batch -d --passphrase-fd 0 ${enc_location} > ~/.aws/credentials

pushd src
mkdir python
cp -R lumigo_tracer.egg-info python/
cp -R lumigo_tracer python/
popd
cp -R src/python/ python/

~/source/utils/common_bash/create_layer.sh lumigo-python-tracer ALL python "python3.6 python3.7"

git add README.md
git commit -m "Update README.md layer ARN"

echo "Create release tag"
push_tags

echo "Done"
