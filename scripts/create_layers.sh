#!/usr/bin/env bash
set -Eeo pipefail

setup_git() {
    git config --global user.email "no-reply@build.com"
    git config --global user.name "CircleCI"
    git checkout master
    # Avoid version failure
    git stash
    git pull origin master
}

echo ".____                  .__                  .__        ";
echo "|    |    __ __  _____ |__| ____   ____     |__| ____  ";
echo "|    |   |  |  \/     \|  |/ ___\ /  _ \    |  |/  _ \ ";
echo "|    |___|  |  /  Y Y  \  / /_/  >  <_> )   |  (  <_> )";
echo "|_______ \____/|__|_|  /__\___  / \____/ /\ |__|\____/ ";
echo "        \/           \/  /_____/         \/            ";
echo
echo "Update Python Tracer Layers"

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

echo "Creating lumigo-python-tracer layer"
./scripts/prepare_layer_files.sh

echo "Creating layer latest version arn table md file (LAYERS.md)"
commit_version="$(git describe --abbrev=0 --tags)"
../utils/common_bash/create_layer.sh \
    --layer-name lumigo-python-tracer \
    --region ALL \
    --package-folder python \
    --version "$commit_version" \
    --runtimes "python3.6 python3.7 python3.8 python3.9 python3.10 python3.11 python3.12 python3.13 python3.14"

cd .. && git clone git@github.com:lumigo-io/larn.git
cd larn && npm i -g
larn -r python3.6 -n layers/LAYERS36 --filter lumigo-python-tracer -p ~/python_tracer
larn -r python3.7 -n layers/LAYERS37 --filter lumigo-python-tracer -p ~/python_tracer
cd ../python_tracer
git add layers/LAYERS36.md
git add layers/LAYERS37.md
git commit -m "layers-table: layers md [skip ci]"
git push origin master

echo "Done"
