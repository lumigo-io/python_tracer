import json
import os
import http.client

from lumigo_tracer.extension.extension_utils import get_extension_logger
from lumigo_tracer.extension.extension import LumigoExtension
from lumigo_tracer.extension.lambda_service import LambdaService
from lumigo_tracer.lumigo_utils import is_kill_switch_on, lumigo_safe_execute, config

STOP_EXTENSION_KEY = "LUMIGO_EXTENSION_STOP"
LUMIGO_EXTENSION_NAME = "lumigo"

REQUESTED_ENVS = [
    "LUMIGO_DEBUG",
    "LUMIGO_TRACER_TOKEN",
    "LUMIGO_TRACER_HOST",
    "LUMIGO_EXTENSION_ON",
    "LUMIGO_SWITCH_OFF",
]
EVENTS = ["INVOKE", "SHUTDOWN"]


def register():  # type: ignore[no-untyped-def]
    body = json.dumps({"events": EVENTS})
    headers = {"Lambda-Extension-Name": LUMIGO_EXTENSION_NAME}
    conn = http.client.HTTPConnection(os.environ["AWS_LAMBDA_RUNTIME_API"])
    conn.request("POST", "/2020-01-01/extension/register", body, headers=headers)
    response = conn.getresponse()
    response.read()

    return response.headers["Lambda-Extension-Identifier"]


def start_extension_loop(lambda_service: LambdaService):  # type: ignore[no-untyped-def]
    with lumigo_safe_execute("Extension main initialization"):
        get_extension_logger().debug(
            f"Extension started running with extension id: {lambda_service.extension_id}"
        )
        config()
        extension = LumigoExtension(lambda_service)
    for event in lambda_service.events_generator():
        get_extension_logger().debug(f"Extension got event: {event}")
        if event.get("eventType") == "INVOKE":
            with lumigo_safe_execute("Extension: start new invocation"):
                extension.start_new_invocation(event)
        elif event.get("eventType") == "SHUTDOWN":
            with lumigo_safe_execute("Extension: shutdown"):
                extension.shutdown()
            break
        else:
            get_extension_logger().error(f"Extension got unknown event: {event}")
    get_extension_logger().debug("Extension finished running")


def start_empty_extension_loop(lambda_service: LambdaService):  # type: ignore[no-untyped-def]
    get_extension_logger().debug("Lumigo extension is disables")
    for event in lambda_service.events_generator():
        if event.get("eventType") == "SHUTDOWN":
            break


def main():  # type: ignore[no-untyped-def]
    extension_id = register()

    lambda_service = LambdaService(extension_id)
    if is_kill_switch_on() or (os.environ.get(STOP_EXTENSION_KEY, "").lower() == "true"):
        start_empty_extension_loop(lambda_service)
    else:
        start_extension_loop(lambda_service)


if __name__ == "__main__":
    main()
