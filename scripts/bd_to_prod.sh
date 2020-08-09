#!/usr/bin/env bash
set -Eeo pipefail

setup_git() {
    git config --global user.email "no-reply@build.com"
    git config --global user.name "CircleCI"
    git checkout master
    # Avoid version failure
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
echo "Deploy Python Tracer"

setup_git

enc_location=../common-resources/encrypted_files/credentials_production.enc
if [[ ! -f ${enc_location} ]]
then
    echo "$enc_location not found"
    exit 1
fi
echo "Creating new credential files"
mkdir -p ~/.aws
echo ${KEY} | gpg --batch -d --passphrase-fd 0 ${enc_location} > ~/.aws/credentials

echo "Getting latest changes from git"
changes=$(git log $(git describe --tags --abbrev=0)..HEAD --oneline)

sudo pip install --upgrade bumpversion
bumpversion patch --message "{current_version} → {new_version}. Changes: ${changes}"

push_tags

echo "Uploading to PyPi"
pip install wheel --quiet
python setup.py bdist_wheel
pip install twine
twine upload dist/*

echo "Creating lumigo-python-tracer layer"
./scripts/prepare_layer_files.sh

echo "Creating layer latest version arn table md file (LAYERS.md)"
../utils/common_bash/create_layer.sh --layer-name lumigo-python-tracer --region ALL --package-folder python --version $(git describe --abbrev=0 --tags) --runtimes "python3.6 python3.7 python3.8"

cd ../larn && npm i -g
larn -r python3.6 -n layers/LAYERS36 --filter lumigo-python-tracer -p ~/python_tracer
larn -r python3.7 -n layers/LAYERS37 --filter lumigo-python-tracer -p ~/python_tracer
cd ../python_tracer
git add layers/LAYERS36.md
git add layers/LAYERS37.md
git commit -m "layers-table: layers md [skip ci]"
git push origin master
echo "Done"
