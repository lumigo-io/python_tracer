import pytest

from lumigo_tracer.extension.sampler import Sampler


@pytest.fixture
def sampler():
    return Sampler()


def test_short_cpu_sampling(sampler, mock_linux_files):
    sampler.start_sampling()
    sampler.stop_sampling()
    assert sampler.get_cpu_samples()


def test_short_memory_sampling(sampler, mock_linux_files):
    sampler.start_sampling()
    sampler.stop_sampling()
    assert sampler.get_memory_samples()


def test_sampling_cpu_happy_flow(sampler, mock_linux_files):
    sampler.start_sampling()
    sampler.sample()
    sampler.sample()
    sampler.stop_sampling()
    assert len(sampler.get_cpu_samples()) == 3


def test_sampling_memory_happy_flow(sampler, mock_linux_files):
    sampler.start_sampling()
    sampler.sample()
    sampler.sample()
    sampler.stop_sampling()
    assert len(sampler.get_memory_samples()) == 4
