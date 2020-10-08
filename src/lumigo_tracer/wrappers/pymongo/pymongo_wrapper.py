import time
import uuid

from lumigo_tracer.lumigo_utils import lumigo_safe_execute, get_logger, lumigo_dumps
from lumigo_tracer.spans_container import SpansContainer

try:
    from pymongo import monitoring
except Exception:
    monitoring = None

if not monitoring:
    LumigoMongoMonitoring = None
else:

    class LumigoMongoMonitoring(monitoring.CommandListener):  # type: ignore
        def started(self, event):
            with lumigo_safe_execute("pymongo started"):
                SpansContainer.get_span().add_span(
                    {
                        "id": str(uuid.uuid4()),
                        "started": int(time.time() / 1000),
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
                last_span = SpansContainer.get_span().get_last_span()
                if not last_span or last_span.get("mongoRequestId") != event.request_id:
                    get_logger().warning("Mongo span ended without a record on its start")
                    return
                last_span.update(
                    {
                        "ended": last_span["started"] + (event.duration_micros / 1000),
                        "response": lumigo_dumps(event.reply),
                    }
                )

        def failed(self, event):
            with lumigo_safe_execute("pymongo succeed"):
                last_span = SpansContainer.get_span().get_last_span()
                if not last_span or last_span.get("mongoRequestId") != event.request_id:
                    get_logger().warning("Mongo span ended without a record on its start")
                    return
                last_span.update(
                    {
                        "ended": last_span["started"] + (event.duration_micros / 1000),
                        "error": lumigo_dumps(event.failure),
                    }
                )


def wrap_pymongo():
    with lumigo_safe_execute("wrap pymogno"):
        if monitoring:
            get_logger().debug("wrapping pymongo")
            monitoring.register(LumigoMongoMonitoring())
