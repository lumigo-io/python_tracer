from typing import Dict
import uuid

from lumigo_tracer.lumigo_utils import (
    lumigo_safe_execute,
    get_logger,
    lumigo_dumps,
    get_current_ms_time,
)
from lumigo_tracer.spans_container import SpansContainer

try:
    from pymongo import monitoring
except Exception:
    monitoring = None

if not monitoring:
    LumigoMongoMonitoring = None
else:

    class LumigoMongoMonitoring(monitoring.CommandListener):  # type: ignore
        request_to_span_id: Dict[str, str] = {}
        MONGO_SPAN = "mongoDb"

        def started(self, event):
            with lumigo_safe_execute("pymongo started"):
                span_id = str(uuid.uuid4())
                LumigoMongoMonitoring.request_to_span_id[event.request_id] = span_id
                SpansContainer.get_span().add_span(
                    {
                        "id": span_id,
                        "type": self.MONGO_SPAN,
                        "started": get_current_ms_time(),
                        "databaseName": event.database_name,
                        "commandName": event.command_name,
                        "request": lumigo_dumps(event.command),
                        "mongoRequestId": event.request_id,
                        "mongoOperationId": event.operation_id,
                        "mongoConnectionId": event.connection_id,
                    }
                )

        def succeeded(self, event):
            with lumigo_safe_execute("pymongo succeed"):
                if event.request_id not in LumigoMongoMonitoring.request_to_span_id:
                    get_logger().warning("Mongo span ended without a record on its start")
                    return
                span_id = LumigoMongoMonitoring.request_to_span_id.pop(event.request_id)
                span = SpansContainer.get_span().get_span_by_id(span_id)
                span.update(
                    {
                        "ended": span["started"] + (event.duration_micros / 1000),
                        "response": lumigo_dumps(event.reply),
                    }
                )

        def failed(self, event):
            with lumigo_safe_execute("pymongo failed"):
                if event.request_id not in LumigoMongoMonitoring.request_to_span_id:
                    get_logger().warning("Mongo span ended without a record on its start")
                    return
                span_id = LumigoMongoMonitoring.request_to_span_id.pop(event.request_id)
                span = SpansContainer.get_span().get_span_by_id(span_id)
                span.update(
                    {
                        "ended": span["started"] + (event.duration_micros / 1000),
                        "error": lumigo_dumps(event.failure),
                    }
                )


def wrap_pymongo():
    with lumigo_safe_execute("wrap pymogno"):
        if monitoring:
            get_logger().debug("wrapping pymongo")
            monitoring.register(LumigoMongoMonitoring())
