from chalice import Chalice  # type: ignore
from lumigo_tracer import lumigo_tracer

app = Chalice(app_name="chalice")


@app.route("/")
def index():
    return {"hello": "world"}


@app.route("/123")
def index2():
    return {"hello": "world"}


app = lumigo_tracer(token="123")(app)
