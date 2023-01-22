from lumigo_tracer.lambda_tracer.global_scope_exec import global_scope_exec
from lumigo_tracer.lambda_tracer import lambda_reporter
from lumigo_tracer import wrappers


def test_global_scope_preparation_called_only_in_lambda(monkeypatch):
    lambda_reporter.edge_connection = None
    wrappers.already_wrapped = False

    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_VERSION", raising=False)
    global_scope_exec()
    assert lambda_reporter.edge_connection is None
    assert not wrappers.already_wrapped

    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_VERSION", "true")
    global_scope_exec()
    assert lambda_reporter.edge_connection is not None
    assert wrappers.already_wrapped
