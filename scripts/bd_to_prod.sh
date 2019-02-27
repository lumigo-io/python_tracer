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

echo "Create release tag"
push_tags
echo "Done"