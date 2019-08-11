import os
from semantic_release.pypi import upload_to_pypi

if __name__ == "__main__":
    try:
        upload_to_pypi(username=os.environ["PYPI_USERNAME"], password=os.environ["PYPI_PASSWORD"])
    except Exception as e:
        print("Failed to publish PYPI package", e)
        exit(1)
