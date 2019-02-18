import os
import setuptools

setuptools.setup(
    name="lumigo_tracer",
    version=open(os.path.join("lumigo_tracer", "VERSION"), "r").read(),
    author="saart",
    author_email="saart@lumigo.io",
    description="Troubleshoot your lambda using lumigo",
    long_description_content_type="text/markdown",
    url="https://github.com/lumigo-io/python_tracer.git",
    packages=setuptools.find_packages(exclude="test"),
    install_requires=[],
    classifiers=["Programming Language :: Python :: 3", "Operating System :: OS Independent"],
    data_files=[("", ["lumigo_tracer/VERSION"])],
)
