"""
Тесты для определения типа форм
"""
import pytest
from app.models.form_model import detect_form_type, FormType


@pytest.mark.parametrize("form_name, expected_type", [
    # 5ФК формы
    ("Отчет 5ФК за 2023 год", FormType.FK_5),
    ("5ФК статистика спорта", FormType.FK_5),
    ("Форма 5ФК-2024", FormType.FK_5),
    ("5фк данные", FormType.FK_5),  # нижний регистр

    # 1ФК формы
    ("Форма 1ФК 2023", FormType.FK_1),
    ("Статистика 1ФК", FormType.FK_1),
    ("Отчет ФК за год", FormType.FK_1),  # просто "ФК"
    ("фк-отчет", FormType.FK_1),  # нижний регистр

    # Неизвестные формы
    ("Произвольная форма", FormType.UNKNOWN),
    ("", FormType.UNKNOWN),
    (None, FormType.UNKNOWN),
    ("Форма 2ФК", FormType.UNKNOWN),  # не 1ФК и не 5ФК
    ("Просто отчет", FormType.UNKNOWN),
])
def test_detect_form_type(form_name, expected_type):
    """Тест определения типа формы по названию"""
    result = detect_form_type(form_name)
    assert result == expected_type, f"Для '{form_name}' ожидался {expected_type}, получен {result}"


def test_form_type_priority():
    """Тест приоритета: 5ФК должен иметь приоритет над 1ФК"""
    # Если есть и 5ФК и 1ФК в названии, должен определяться как 5ФК
    assert detect_form_type("5ФК и 1ФК отчет") == FormType.FK_5
    assert detect_form_type("1ФК и 5ФК данные") == FormType.FK_5


def test_form_type_case_insensitive():
    """Тест нечувствительности к регистру"""
    assert detect_form_type("5фк отчет") == FormType.FK_5
    assert detect_form_type("5ФК ОТЧЕТ") == FormType.FK_5
    assert detect_form_type("Фк данные") == FormType.FK_1
    assert detect_form_type("фК ОТЧЕТ") == FormType.FK_1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])