import urllib.request

from mock import Mock


def test_events_generator_async(monkeypatch, lambda_service):
    http_mock = Mock()
    http_mock.read.side_effect = ['{"first": "call"}', '{"second": "call"}']
    monkeypatch.setattr(urllib.request, "urlopen", lambda *args: http_mock)
    result = list(lambda_service.events_generator())
    assert result[0] == {"first": "call"}
    assert result[1] == {"second": "call"}


def test_ready_for_next_event_non_blocking(monkeypatch, lambda_service):
    done = False

    def mocked_http(called):
        called()
        while not done:
            pass

    called_mock = Mock()
    monkeypatch.setattr(urllib.request, "urlopen", lambda *args: mocked_http(called_mock))
    lambda_service.ready_for_next_event()
    called_mock.assert_called_once()
    done = True
