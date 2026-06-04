"""Тесты нормализации value для FlatData."""

from app.domain.flat_data.value_utils import normalize_flat_value


def test_numeric_passthrough():
    assert normalize_flat_value(42) == 42.0
    assert normalize_flat_value(3.14) == 3.14


def test_non_numeric_string_to_zero():
    assert normalize_flat_value("X") == 0.0
    assert normalize_flat_value("текст") == 0.0


def test_parseable_string():
    assert normalize_flat_value("12,5") == 12.5
    assert normalize_flat_value(" 7 ") == 7.0


def test_none_and_empty_to_zero():
    assert normalize_flat_value(None) == 0.0
    assert normalize_flat_value("") == 0.0
    assert normalize_flat_value("nan") == 0.0
