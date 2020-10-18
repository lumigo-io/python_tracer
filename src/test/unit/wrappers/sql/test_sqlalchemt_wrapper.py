import pytest
from sqlalchemy.exc import OperationalError

from lumigo_tracer.spans_container import SpansContainer
from lumigo_tracer.tracer import lumigo_tracer
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.sql import select


md = MetaData()
Users = Table("users", md, Column("id", Integer, primary_key=True), Column("name", String))


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "file.db"
    engine = create_engine(fr"sqlite:///{path}")
    md.create_all(engine)
    yield f"sqlite:///{path}"


def test_happy_flow(context, db):
    @lumigo_tracer(token="123")
    def lambda_test_function(event, context):
        engine = create_engine(db)
        conn = engine.connect()
        conn.execute(Users.insert().values(name="saart"))
        result = conn.execute(select([Users]))
        return result.fetchone()

    assert lambda_test_function({}, context) == (1, "saart")
    http_spans = SpansContainer.get_span().spans

    assert len(http_spans) == 2
    assert http_spans[0]["query"] == "INSERT INTO users (name) VALUES (?)"
    assert http_spans[0]["values"] == '["saart"]'
    assert http_spans[0]["ended"] >= http_spans[0]["started"]

    assert http_spans[1]["query"] == "SELECT users.id, users.name \nFROM users"
    assert http_spans[1]["values"] == "[]"
    assert http_spans[0]["ended"] >= http_spans[0]["started"]


def test_non_existing_table(context, db):
    @lumigo_tracer(token="123")
    def lambda_test_function(event, context):
        others = Table("others", md, Column("id", Integer, primary_key=True))
        engine = create_engine(db)
        conn = engine.connect()
        result = conn.execute(select([others]))
        return result.fetchone()

    with pytest.raises(OperationalError):
        lambda_test_function({}, context)

    http_spans = SpansContainer.get_span().spans

    assert len(http_spans) == 1
    assert http_spans[0]["query"] == "SELECT others.id \nFROM others"
    assert (
        http_spans[0]["error"] == '{"type": "OperationalError", "args": ["no such table: others"]}'
    )
    assert http_spans[0]["ended"] >= http_spans[0]["started"]
