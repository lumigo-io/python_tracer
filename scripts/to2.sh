source ./venv/bin/activate

pushd src

pip install future-fstrings 3to2 strip-hints
for file in $(find src -type f); do
    # don't use f-strings
    future-fstrings-show "$file" > "$file.tmp";
    # add future print
    sed -i '' '1s/^/from __future__ import print_function\
/' "$file.tmp";
    # remove the typing imports
    sed -i '' '/from typing import.*/d' "$file.tmp";
    # no types hints
    strip-hints "$file.tmp" > "$file";
    rm "$file.tmp";
done
# change imports, exceptions, bytes, class(object), etc.
3to2 -n -w src;

sed -i '' 's/u\"/\"/g' lumigo_tracer/libs/wrapt.py;
sed -i '' 's/urllib.request/urllib2/g' lumigo_tracer/utils.py;
sed -i '' 's/from collections.abc import Iterable/from collections import Iterable/g' lumigo_tracer/parsers/utils.py;
sed -i '' 's/900_000/900000/g' lumigo_tracer/parsers/utils.py;
sed -i '' 's/urllib.parse/urllib/g' lumigo_tracer/parsers/utils.py;
sed -i '' 's/**self.lumigo_conf_kwargs,/**self.lumigo_conf_kwargs/g' lumigo_tracer/sync_http/sync_hook.py;

popd


#brew install pypy
if [[ ! -d ./pypy_env ]]; then
    echo "creating new virtualenv - pypy"
    virtualenv -p pypy pypy_env
fi
source ./pypy_env/bin/activate

pip install pytest capturer
python setup.py develop

py.test
