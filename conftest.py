# conftest.py  (положить в корень репозитория)
import os
import sys
import logging
import pytest

# 1) Добавляем корень проекта в sys.path (чтобы import main, config и т.д. работал)
ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# 2) Если не задана переменная окружения для тестовой БД — подставляем безопасные значения
os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
os.environ.setdefault("DATABASE_NAME", "sport_data_test")
os.environ.setdefault("APP_ENV", "testing")
os.environ.setdefault("API_HOST", "127.0.0.1")
os.environ.setdefault("API_PORT", "2700")
os.environ.setdefault("DEBUG", "True")

# 3) Настройка логирования (видим операции в консоли при pytest -s)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)


print(f"[TEST-BOOT] project root added to sys.path: {ROOT}")
print(f"[TEST-BOOT] MONGO_URI={os.environ.get('MONGO_URI', 'not set')}, DATABASE_NAME={os.environ.get('DATABASE_NAME', 'not set')}")


def pytest_collection_modifyitems(config, items):
    """
    Автоматически проставляет метки 'unit' и 'integration' в зависимости от пути теста.
    Это избавляет от необходимости добавлять @pytest.mark.* в каждый тест.
    """
    for item in items:
        p = str(item.fspath)
        # если тест лежит в tests/unit/ -> пометить unit
        if os.path.sep + "tests" + os.path.sep + "unit" + os.path.sep in p:
            item.add_marker(pytest.mark.unit)
        # если тест лежит в tests/integration/ -> пометить integration
        elif os.path.sep + "tests" + os.path.sep + "integration" + os.path.sep in p:
            item.add_marker(pytest.mark.integration)
        # если тест лежит в tests/client/ -> пометить client
        elif os.path.sep + "tests" + os.path.sep + "client" + os.path.sep in p:
            item.add_marker(pytest.mark.client)