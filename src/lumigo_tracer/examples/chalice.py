from chalice import Chalice  # type: ignore
from lumigo_tracer import LumigoChalice

app = Chalice(app_name="chalice")
app = LumigoChalice(app, token="123")


@app.route("/")
def index():
    return {"hello": "world"}


@app.route("/123")
def index2():
    return {"hello": "world"}
