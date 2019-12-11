#!/usr/bin/env bash

pip install wheel
python setup.py bdist_wheel
rm -rf python && mkdir python
cp -R src/lumigo_tracer.egg-info python/
cp -R src/lumigo_tracer python/
