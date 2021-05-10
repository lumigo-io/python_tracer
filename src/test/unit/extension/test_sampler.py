import pytest

from lumigo_tracer.extension.sampler import CpuSampler


@pytest.fixture
def sampler():
    return CpuSampler()


def test_short_sampling(sampler, mock_linux_files):
    sampler.start_sampling()
    sampler.stop_sampling()
    assert sampler.get_samples()


def test_sampling_happy_flow(sampler, mock_linux_files):
    sampler.start_sampling()
    sampler.sample()
    sampler.sample()
    sampler.stop_sampling()
    assert len(sampler.get_samples()) == 3
