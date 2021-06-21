import pytest
import signal
import time
from mock import mock_open, patch, Mock

from lumigo_tracer.lumigo_utils import LUMIGO_TOKEN_KEY
from lumigo_tracer.extension.lambda_service import LambdaService


@pytest.fixture(autouse=True)
def extension_env(monkeypatch):
    monkeypatch.setenv("AWS_LAMBDA_RUNTIME_API", "127.0.0.1")
    monkeypatch.setenv(LUMIGO_TOKEN_KEY, "t_123")


@pytest.fixture(autouse=True)
def sampler_timer(monkeypatch):
    monkeypatch.setattr(signal, "signal", Mock())
    monkeypatch.setattr(signal, "alarm", Mock())
    monkeypatch.setattr(signal, "setitimer", Mock())


@pytest.fixture
def mock_linux_files():
    m = mock_open()
    m().readlines.return_value = [f"cpu {int(time.time() * 10000)} 34 2290 22625563 6290 127 456"]
    m().read.return_value = (
        f"IpExt: 0 0 0 0 277959 0 {int(time.time() * 10000)} 1234 0 0 58649349 0 0 0 0 0"
    )
    with patch("lumigo_tracer.extension.extension_utils.open", m):
        yield


def clean_folder(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


@pytest.fixture
def lambda_service():
    return LambdaService("eid")
