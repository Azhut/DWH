# tests/conftest.py
import pytest
from fastapi.testclient import TestClient

@pytest.fixture(scope="session")
def client():
    """
    Используем реальные Pydantic Settings (считываются из .env или Docker env).
    """
    from main import create_app
    app = create_app()
    with TestClient(app) as c:
        yield c
