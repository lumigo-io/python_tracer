#!/usr/bin/env bash
echo "Create Layer"
enc_location=../common-resources/encrypted_files/credentials_production.enc
if [[ ! -f ${enc_location} ]]
then
    echo "$enc_location not found"
    exit 1
fi
echo "Creating new credential files"

pushd src
mkdir python
cp -R lumigo_tracer.egg-info python/
cp -R lumigo_tracer python/
popd
cp -R src/python/ python/

../utils/common_bash/create_layer.sh --layer-name lumigo-python-tracer --region us-west-2 --package-folder python --version $(git describe --abbrev=0 --tags) --runtimes "python3.6 python3.7 python3.8"
