echo "Extension layer"
python -W ignore setup.py --quiet bdist_wheel  # create .egg-info
pushd src > /dev/null || exit
echo "-create directories to zip"
echo "--extension"
mkdir extensions
cp lumigo_tracer/extension/bootstrap/lumigo extensions/
echo "--tracer"
mkdir extension-python-modules
cp -R lumigo_tracer.egg-info extension-python-modules/
cp -R lumigo_tracer extension-python-modules/
echo "--python runtime"
aws s3 cp --quiet s3://lumigo-runtimes/python/lean-python-runtime-37.zip runtime.zip
unzip -q runtime.zip
echo "--special temp file"
touch preview-extensions-ggqizro707
echo "-zipping"
zip -qr "extensions.zip" "extensions" "extension-python-modules" "python-runtime" "preview-extensions-ggqizro707"  # take all the directory
echo "-publish"
../utils/common_bash/create_layer.sh --layer-name "extensions-layer" --region ALL --package-folder "extensions.zip" --version $(git describe --abbrev=0 --tags) --runtimes  ""
rm -rf extensions extension-python-modules extensions.zip runtime.zip python-runtime __MACOSX
popd > /dev/null || exit

echo "\nDone.\n"

