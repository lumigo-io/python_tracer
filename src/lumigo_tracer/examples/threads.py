import concurrent.futures
from types import SimpleNamespace
import urllib.request

from lumigo_tracer import lumigo_tracer


COUNT = 3


def to_exec(i):
    urllib.request.urlopen(f"https://postman-echo.com/get?foo={i}").read()


@lumigo_tracer(token="t_12345678910111213")
def my_lambda(event, context):
    with concurrent.futures.ThreadPoolExecutor(max_workers=COUNT) as executor:
        futures = [executor.submit(to_exec, i) for i in range(COUNT)]
        [f.result() for f in futures]
    return {"hello": "world"}


if __name__ == "__main__":
    context = SimpleNamespace(aws_request_id="1234", get_remaining_time_in_millis=lambda: 1000 * 2)
    my_lambda({}, context)
