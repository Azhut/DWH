# tests/integration/conftest.py
import pytest
from fastapi.testclient import TestClient
from main import create_app
import asyncio


@pytest.fixture
def client():
    """Создает синхронный TestClient для тестов"""
    app = create_app()
    with TestClient(app) as client:
        yield client


@pytest.fixture
async def async_client():
    """Асинхронный клиент для тестов (альтернатива)"""
    app = create_app()
    from httpx import AsyncClient
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client


@pytest.fixture
def test_excel_file(tmp_path):
    """
    Создаём временный XLSX-файл с минимальными листами (аналог data_example.xlsx по структуре)
    """
    import pandas as pd
    import os

    file_path = tmp_path / "test_data.xlsx"
    df1 = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    df2 = pd.DataFrame({"X": [10], "Y": [20]})
    with pd.ExcelWriter(file_path) as writer:
        df1.to_excel(writer, sheet_name="Раздел1", index=False)
        df2.to_excel(writer, sheet_name="Раздел2", index=False)
    return file_path