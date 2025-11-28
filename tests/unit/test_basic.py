"""
Базовые тесты для проверки работы pytest
"""


def test_basic_math():
    """Тест базовой математики"""
    assert 1 + 1 == 2


def test_string_operations():
    """Тест строковых операций"""
    assert "hello".upper() == "HELLO"


def test_list_operations():
    """Тест операций со списками"""
    items = [1, 2, 3]
    assert len(items) == 3
    assert 2 in items


class TestBasicClass:
    """Базовый тестовый класс"""

    def test_in_class(self):
        """Тест внутри класса"""
        assert True is True