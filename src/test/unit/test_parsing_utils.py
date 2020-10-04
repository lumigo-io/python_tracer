import os

import pytest

from lumigo_tracer.parsing_utils import (
    should_scrub_domain,
    str_to_list,
    str_to_tuple,
    safe_split_get,
    safe_key_from_json,
    safe_get,
    recursive_json_join,
    parse_trace_id,
    safe_key_from_query,
    safe_key_from_xml,
)
from lumigo_tracer.lumigo_utils import config, Configuration


@pytest.mark.parametrize(
    ("input_params", "expected_output"),
    [
        (("a.b.c", ".", 0), "a"),  # happy flow
        (("a.b.c", ".", 1), "b"),
        (("a.b.c", ".", 5, "d"), "d"),  # return the default
    ],
)
def test_safe_split_get(input_params, expected_output):
    assert safe_split_get(*input_params) == expected_output


@pytest.mark.parametrize(
    ("input_params", "expected_output"),
    [
        ((b'{"a": "b"}', "a"), "b"),  # happy flow
        ((b'{"a": "b"}', "c"), None),  # return the default
        ((b"<a>b</a>", "c"), None),  # not a json
    ],
)
def test_key_from_json(input_params, expected_output):
    assert safe_key_from_json(*input_params) == expected_output


@pytest.mark.parametrize(
    ("input_params", "expected_output"),
    [
        ((b"<a>b</a>", "a"), "b"),  # happy flow - one parameter
        ((b"<a><b>c</b><d></d></a>", "a/b"), "c"),  # happy flow - longer path
        ((b"<a>b</a>", "c"), None),  # not existing key
        ((b"<a><b>c</b></a>", "a/e"), None),  # not existing sub-key
        ((b'{"a": "b"}', "c"), None),  # not an xml
    ],
)
def test_key_from_xml(input_params, expected_output):
    assert safe_key_from_xml(*input_params) == expected_output


@pytest.mark.parametrize(
    ("input_params", "expected_output"),
    [
        ((b"a=b", "a"), "b"),  # happy flow - one parameter
        ((b"a=b&c=d", "c"), "d"),  # happy flow - multiple parameters
        ((b"a=b&c=d", "e"), None),  # not existing key
        ((b'{"a": "b"}', "c"), None),  # not an query, no '&'
        ((b"a&b", "a"), None),  # not an query, with '&'
    ],
)
def test_key_from_query(input_params, expected_output):
    assert safe_key_from_query(*input_params) == expected_output


@pytest.mark.parametrize(
    ("trace_id", "result"),
    [
        ("Root=1-2-3;Parent=34;Sampled=0", ("1-2-3", "3", ";Parent=34;Sampled=0")),  # happy flow
        ("Root=1-2-3;", ("1-2-3", "3", ";")),
        ("Root=1-2;", ("1-2", "", ";")),
        ("a;1", ("", "", ";1")),
        ("123", ("", "", "123")),
    ],
)
def test_parse_trace_id(trace_id, result):
    assert parse_trace_id(trace_id) == result


@pytest.mark.parametrize(
    ("d1", "d2", "result"),
    [
        ({1: 2}, {3: 4}, {1: 2, 3: 4}),  # happy flow
        ({1: 2}, {1: 3}, {1: 2}),  # same key twice
        ({1: {2: 3}}, {4: 5}, {1: {2: 3}, 4: 5}),  # dictionary in d1 and nothing in d2
        ({1: {2: 3}}, {1: {4: 5}}, {1: {2: 3, 4: 5}}),  # merge two inner dictionaries
    ],
)
def test_recursive_json_join(d1, d2, result):
    assert recursive_json_join(d1, d2) == result


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
    ("d", "keys", "result_value", "default"),
    [
        ({"k": ["a", "b"]}, ["k", 1], "b", None),  # Happy flow.
        ({"k": ["a"]}, ["k", 1], "default", "default"),  # List index out of range.
        ({"k": "a"}, ["b"], "default", "default"),  # Key doesn't exist.
        ({"k": "a"}, [1], "default", "default"),  # Wrong key type.
        ({"k": "a"}, ["k", 0, 1], "default", "default"),  # Wrong keys length.
    ],
)
def test_safe_get(d, keys, result_value, default):
    assert safe_get(d, keys, default) == result_value


@pytest.mark.parametrize(
    ("regexes", "url", "expected"),
    [(["secret.*"], "lumigo.io", False), (["not-relevant", "secret.*"], "secret.aws.com", True)],
)
def test_should_scrub_domain(regexes, url, expected):
    config(domains_scrubber=regexes)
    assert should_scrub_domain(url) == expected


def test_str_to_list():
    assert str_to_list("a,b,c,d") == ["a", "b", "c", "d"]


def test_str_to_list_exception():
    assert str_to_list("") is None


def test_str_to_tuple():
    assert str_to_tuple("a,b,c,d") == ("a", "b", "c", "d")


def test_str_to_tuple_exception():
    assert str_to_tuple([]) is None
