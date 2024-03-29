import uuid
from typing import Dict

from lumigo_tracer.lambda_tracer.lambda_reporter import MONGO_SPAN
from lumigo_tracer.lambda_tracer.spans_container import SpansContainer
from lumigo_tracer.lumigo_utils import (
    get_current_ms_time,
    get_logger,
    lumigo_dumps,
    lumigo_safe_execute,
)

try:
    from pymongo import monitoring
except Exception:
    monitoring = None

if monitoring:

    class LumigoMongoMonitoring(monitoring.CommandListener):
        request_to_span_id: Dict[str, str] = {}

        def started(self, event):  # type: ignore[no-untyped-def]
            with lumigo_safe_execute("pymongo started"):
                span_id = str(uuid.uuid4())
                LumigoMongoMonitoring.request_to_span_id[event.request_id] = span_id
                SpansContainer.get_span().add_span(
                    {
                        "id": span_id,
                        "type": MONGO_SPAN,
                        "started": get_current_ms_time(),
                        "databaseName": event.database_name,
                        "commandName": event.command_name,
                        "request": lumigo_dumps(event.command),
                        "mongoRequestId": event.request_id,
                        "mongoOperationId": event.operation_id,
                        "mongoConnectionId": event.connection_id,
                    }
                )

        def succeeded(self, event):  # type: ignore[no-untyped-def]
            with lumigo_safe_execute("pymongo succeed"):
                if event.request_id not in LumigoMongoMonitoring.request_to_span_id:
                    get_logger().warning("Mongo span ended without a record on its start")
                    return
                span_id = LumigoMongoMonitoring.request_to_span_id.pop(event.request_id)
                span = SpansContainer.get_span().get_span_by_id(span_id)
                span.update(  # type: ignore[union-attr]
                    {
                        "ended": span["started"] + (event.duration_micros / 1000),  # type: ignore[index]
                        "response": lumigo_dumps(event.reply),
                    }
                )

        def failed(self, event):  # type: ignore[no-untyped-def]
            with lumigo_safe_execute("pymongo failed"):
                if event.request_id not in LumigoMongoMonitoring.request_to_span_id:
                    get_logger().warning("Mongo span ended without a record on its start")
                    return
                span_id = LumigoMongoMonitoring.request_to_span_id.pop(event.request_id)
                span = SpansContainer.get_span().get_span_by_id(span_id)
                span.update(  # type: ignore[union-attr]
                    {
                        "ended": span["started"] + (event.duration_micros / 1000),  # type: ignore[index]
                        "error": lumigo_dumps(event.failure),
                    }
                )


else:
    LumigoMongoMonitoring = None  # type: ignore


def wrap_pymongo():  # type: ignore[no-untyped-def]
    with lumigo_safe_execute("wrap pymogno"):
        if monitoring:
            get_logger().debug("wrapping pymongo")
            monitoring.register(LumigoMongoMonitoring())
