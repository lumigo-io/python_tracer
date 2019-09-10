source ./venv/bin/activate

pushd src

pip install future-fstrings 3to2 strip-hints
for file in $(find {lumigo_tracer,test} -type f); do
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
# change imports, exceptions, bytes, class(object), etc. Dont change: print("a") -> print "a", str -> unicode.
3to2 ./ -n -w -x print -x str;
sleep 5;

sed -i '' 's/urllib.request/urllib2/g' lumigo_tracer/utils.py;
sed -i '' 's/from collections.abc import Iterable/from collections import Iterable/g' lumigo_tracer/parsers/utils.py;
sed -i '' 's/except json.JSONDecodeError/except ValueError/g' lumigo_tracer/parsers/utils.py;
sed -i '' 's/900_000/900000/g' lumigo_tracer/utils.py;
sed -i '' 's/100_000/100000/g' lumigo_tracer/utils.py;
sed -i '' 's/FrameVariables = Dict[str, str]//g' lumigo_tracer/utils.py;
sed -i '' 's/exec("a{} = 'A'".format((i)))/exec("a{} = 'A'".format((i))) in globals(), locals()/g' lumigo_tracer/utils.py;
sed -i '' 's/urllib.parse/urllib/g' lumigo_tracer/parsers/utils.py;
sed -i '' 's/**self.lumigo_conf_kwargs,/**self.lumigo_conf_kwargs/g' lumigo_tracer/sync_http/sync_hook.py;
sed -i '' 's/**additional_info,/**additional_info/g' lumigo_tracer/spans_container.py;
sed -i '' 's/.hex()/.encode("hex")/g' lumigo_tracer/spans_container.py;
sed -i '' '/from __future__ import absolute_import/d' lumigo_tracer/libs/xmltodict.py;
sed -i '' 's/1_000_000/1000000/g' test/unit/test_main_utils.py;
sed -i '' 's/import urllib/import urllib2/g' test/unit/sync_http/test_sync_hook.py;
sed -i '' 's/import urllib23/import urllib3/g' test/unit/sync_http/test_sync_hook.py;
# change backend to remove the next line
sed -i '' 's/FALLBACK_RUNTIME = "provided"/FALLBACK_RUNTIME = "pypy27 (python)"/g' lumigo_tracer/spans_container.py;

popd


#brew install pypy
if [[ ! -d ./pypy_env ]]; then
    echo "creating new virtualenv - pypy"
    virtualenv -p pypy pypy_env
fi
source ./pypy_env/bin/activate

pip install pytest capturer mock boto3 urllib3
python setup.py develop

py.test
