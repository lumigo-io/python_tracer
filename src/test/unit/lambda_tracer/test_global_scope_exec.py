from lumigo_tracer.lambda_tracer import lambda_reporter
from lumigo_tracer.lambda_tracer.global_scope_exec import global_scope_exec


def test_global_scope_preparation_called_only_in_lambda(monkeypatch):
    lambda_reporter.edge_connection = None

    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_VERSION", raising=False)
    global_scope_exec()
    assert lambda_reporter.edge_connection is None

    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_VERSION", "true")
    global_scope_exec()
    assert lambda_reporter.edge_connection is not None
