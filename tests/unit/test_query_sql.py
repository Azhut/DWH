"""Тесты преобразования Mongo-фильтра в SQL для DuckDB."""

from app.domain.flat_data.query_sql import mongo_filter_to_sql


def test_empty_query():
    sql, params = mongo_filter_to_sql({})
    assert sql == "1=1"
    assert params == []


def test_form_and_in():
    sql, params = mongo_filter_to_sql(
        {"$and": [{"form": "abc"}, {"year": {"$in": [2024, 2025]}}]}
    )
    assert "form = ?" in sql
    assert "year IN (?, ?)" in sql
    assert params == ["abc", 2024, 2025]


def test_regex_ilike():
    sql, params = mongo_filter_to_sql({"row": {"$regex": "орган", "$options": "i"}})
    assert '"row" ILIKE ?' in sql
    assert params == ["%орган%"]
