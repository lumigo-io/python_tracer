import setuptools
import os
import re

with open(
    os.path.join(os.path.dirname(__file__), "lumigo_tracer", "version.py"), "rt"
) as version_file:
    version = re.search(r"version = \"(.*?)\"", version_file.read()).group(1)  # type: ignore

setuptools.setup(
    name="lumigo_tracer",
    version=version,
    author="saart",
    author_email="saart@lumigo.io",
    description="Troubleshoot your lambda using lumigo",
    long_description_content_type="text/markdown",
    url="https://github.com/lumigo-io/python_tracer.git",
    packages=setuptools.find_packages(exclude="test"),
    install_requires=[],
    classifiers=["Programming Language :: Python :: 3", "Operating System :: OS Independent"],
    package_data={"lumigo_tracer": ["VERSION"]},
)
