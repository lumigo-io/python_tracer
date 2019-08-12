#!/usr/bin/env bash
set -Eeo pipefail

setup_git() {
    git config --global user.email "no-reply@build.com"
    git config --global user.name "CircleCI"
    git checkout master
    # Avoid npm version failure
    git stash
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

echo "Uploading to gemfury"
echo "Setup"
pushd src
python setup.py sdist
popd

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
rm -rf python
mkdir python
cp -R lumigo_tracer.egg-info python/
cp -R lumigo_tracer python/
popd
cp -R src/python/ python/

../utils/common_bash/create_layer.sh lumigo-python-tracer ALL python "python3.6 python3.7"
git add README.md
git commit -m "Update README.md layer ARN"

pip install --upgrade wheel setuptools twine pkginfo
pip install python-semantic-release

semantic-release publish
echo "Done"
