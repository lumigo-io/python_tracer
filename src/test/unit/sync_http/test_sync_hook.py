from lumigo_tracer.sync_http.sync_hook import lumigo_lambda
import http.client


def events_by_mock(reporter_mock):
    return reporter_mock.call_args[1]["msgs"]


def test_lambda_wrapper_basic_events(reporter_mock):
    """
    This test checks that the basic events (start and end messages) has been sent.
    """

    @lumigo_lambda
    def lambda_test_function():
        pass

    lambda_test_function()
    events = events_by_mock(reporter_mock)
    assert len(events) == 1
    assert "started" in events[0]
    assert "ended" in events[0]


def test_lambda_wrapper_exception(reporter_mock):
    @lumigo_lambda
    def lambda_test_function():
        raise ValueError("Oh no")

    try:
        lambda_test_function()
    except ValueError:
        pass
    else:
        assert False

    events = events_by_mock(reporter_mock)
    assert len(events) == 1
    assert events[0].get("error", "").startswith("ValueError")


def test_lambda_wrapper_http(reporter_mock):
    @lumigo_lambda
    def lambda_test_function():
        http.client.HTTPConnection("www.google.com").request("POST", "/")

    lambda_test_function()
    events = events_by_mock(reporter_mock)
    assert len(events) == 2
    assert events[1].get("info", {}).get("httpInfo", {}).get("host") == "www.google.com"
