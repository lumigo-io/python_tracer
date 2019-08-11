import setuptools

from src.lumigo_tracer.version import version

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
    license="Apache License 2.0",
    classifiers=["Programming Language :: Python :: 3", "Operating System :: OS Independent"],
    long_description=open("README.md").read(),
)
