import os

import setuptools

VERSION_PATH = os.path.join(os.path.dirname(__file__), "src", "lumigo_tracer", "VERSION")

setuptools.setup(
    name="lumigo_tracer",
    version=open(VERSION_PATH).read(),
    author="Lumigo LTD (https://lumigo.io)",
    author_email="support@lumigo.io",
    description="Lumigo Tracer for Python v3.6 / 3.7 / 3.8 / 3.9 / 3.10 / 3.11 / 3.12 / 3.13 / 3.14 runtimes",
    long_description_content_type="text/markdown",
    url="https://github.com/lumigo-io/python_tracer.git",
    package_dir={"": "src"},
    packages=setuptools.find_packages("src"),
    install_requires=["lumigo_core==0.0.16"],
    license="Apache License 2.0",
    classifiers=["Programming Language :: Python :: 3", "Operating System :: OS Independent"],
    long_description=open("README.md").read(),
    package_data={"lumigo_tracer": ["VERSION"]},
)
