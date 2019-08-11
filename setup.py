import setuptools
import os

from src.lumigo_tracer import version

with open(os.path.join(os.path.dirname(__file__), "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setuptools.setup(
    name="lumigo_tracer",
    version=version,
    author="Lumigo LTD (https://lumigo.io)",
    author_email="support@lumigo.io",
    description="Lumigo Tracer for Python v3.6 / v3.7 runtimes",
    long_description_content_type="text/markdown",
    url="https://github.com/lumigo-io/python_tracer.git",
    packages=setuptools.find_packages(exclude="test"),
    install_requires=[],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Apache-2.0",
    ],
    long_description=long_description,
)
