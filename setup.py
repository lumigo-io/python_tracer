import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="lumigo_tracer",
    version="0.1",
    author="saart",
    author_email="saart@lumigo.io",
    description="Troubleshoot your lambda using lumigo",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lumigo-io/lumigo_tracer.git",
    package_dir={"": "src"},
    packages=["lumigo_tracer"],
    install_requires=["aioboto3", "aiohttp", "boto3"],
    classifiers=["Programming Language :: Python :: 3", "Operating System :: OS Independent"],
)
