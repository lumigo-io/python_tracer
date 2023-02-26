#!/usr/bin/env bash

pip install wheel --quiet
python setup.py bdist_wheel
rm -rf python && mkdir python
cp -R src/lumigo_tracer.egg-info python/
cp -R src/lumigo_tracer python/
python -m pip install -r src/lumigo_tracer.egg-info/requires.txt
cp -R "$(python -c "import lumigo_core; print(lumigo_core.__path__[0])")"* python/
