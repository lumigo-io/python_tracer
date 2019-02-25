import pytest

from lumigo_tracer.parsers import utils


@pytest.mark.parametrize(
    ("input_params", "expected_output"),
    [
        (("a.b.c", ".", 0), "a"),  # happy flow
        (("a.b.c", ".", 1), "b"),
        (("a.b.c", ".", 5, "d"), "d"),  # return the default
    ],
)
def test_safe_split_get(input_params, expected_output):
    assert utils.safe_split_get(*input_params) == expected_output


@pytest.mark.parametrize(
    ("input_params", "expected_output"),
    [
        ((b'{"a": "b"}', "a"), "b"),  # happy flow
        ((b'{"a": "b"}', "c"), None),  # return the default
        ((b"<a>b</a>", "c"), None),  # not a json
    ],
)
def test_key_from_json(input_params, expected_output):
    assert utils.safe_key_from_json(*input_params) == expected_output


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
    assert utils.safe_key_from_xml(*input_params) == expected_output


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
    assert utils.safe_key_from_query(*input_params) == expected_output


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
    assert utils.parse_trace_id(trace_id) == result


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
    assert utils.recursive_json_join(d1, d2) == result
