import pytest
from app.data.services.filter_service import _map_filter_name, _build_query
from app.core.exceptions import log_and_raise_http
from fastapi import HTTPException

def test_map_filter_name_known():
    assert _map_filter_name("год") == "year"
    assert _map_filter_name("ГОРОД") == "city"
    assert _map_filter_name("колонка") == "column"

def test_map_filter_name_unknown():
    with pytest.raises(KeyError):
        _map_filter_name("неизвестно")

def test_build_query_basic():
    filters = [{"filter-name": "год", "values": [2020]}, {"filter-name": "город", "values": ["Msk"]}]
    q = _build_query(filters)
    # Ожидаем, что город преобразуется в верхний регистр
    assert "$and" in q and isinstance(q["$and"], list)
    # Проверим, что в запросе есть оба условия
    fields = {list(cond.keys())[0] for cond in q["$and"]}
    assert {"year", "city"} == fields

def test_build_query_empty_values():
    q = _build_query([{"filter-name": "год", "values": []}])
    assert q == {}

def test_log_and_raise_http_logs_and_raises(caplog):
    with pytest.raises(HTTPException) as excinfo:
        log_and_raise_http(418, "Test Error", exc=ValueError("oops"))
    assert excinfo.value.status_code == 418
    assert "Test Error" in str(excinfo.value.detail)
    # Лог должен содержать сообщение об ошибке
    assert "Test Error" in caplog.text
