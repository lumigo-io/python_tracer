import os

import pytest

from lumigo_tracer.lumigo_utils import Configuration, config
from lumigo_tracer.parsing_utils import should_scrub_domain


def test_config_with_verbose_param_with_no_env_verbose_verbose_is_false():
    config(verbose=False)

    assert Configuration.verbose is False


def test_config_no_verbose_param_and_no_env_verbose_is_true():
    config()

    assert Configuration.verbose


def test_config_no_verbose_param_and_with_env_verbose_equals_to_false_verbose_is_false(monkeypatch):
    monkeypatch.setattr(os, "environ", {"LUMIGO_VERBOSE": "FALSE"})
    config()

    assert Configuration.verbose is False


@pytest.mark.parametrize(
    ("regexes", "url", "expected"),
    [(["secret.*"], "lumigo.io", False), (["not-relevant", "secret.*"], "secret.aws.com", True)],
)
def test_should_scrub_domain(regexes, url, expected):
    config(domains_scrubber=regexes)
    assert should_scrub_domain(url) == expected
