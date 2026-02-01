"""
Тесты для фабрики парсеров
"""
import pytest
from app.parsers.parser_factory import ParserFactory
from app.domain.form.models import FormType
from app.parsers.five_fk_parser import FiveFKParser
from app.parsers.universal_parser import UniversalParser


def test_factory_for_fk1():
    """Тест создания парсеров для 1ФК"""
    # Для известных листов 1ФК должны создаваться соответствующие парсеры
    parser = ParserFactory.create_parser("Раздел0", FormType.FK_1)
    assert parser is not None
    # Проверяем, что это не универсальный парсер
    assert not isinstance(parser, UniversalParser)

    parser = ParserFactory.create_parser("Раздел1", FormType.FK_1)
    assert parser is not None


def test_factory_for_fk5():
    """Тест создания парсеров для 5ФК"""
    # Для 5ФК всегда должен создаваться FiveFKParser
    parser = ParserFactory.create_parser("Любой лист", FormType.FK_5)
    assert isinstance(parser, FiveFKParser)

    # Проверяем, что название листа сохраняется
    assert parser.sheet_name == "Любой лист"


def test_factory_for_unknown():
    """Тест создания парсеров для неизвестных типов"""
    parser = ParserFactory.create_parser("Лист1", FormType.UNKNOWN)
    assert isinstance(parser, UniversalParser)

    # Проверяем, что название листа сохраняется
    assert parser.sheet_name == "Лист1"


def test_factory_missing_fk1_parser():
    """Тест для случая, когда для листа 1ФК нет специфичного парсера"""
    # Для несуществующего листа 1ФК должен создаваться универсальный парсер
    parser = ParserFactory.create_parser("Неизвестный раздел", FormType.FK_1)
    # В текущей реализации это будет универсальный парсер
    assert parser is not None


def test_get_available_parsers():
    """Тест получения доступных парсеров"""
    # Для 1ФК должен возвращаться словарь с парсерами
    parsers_fk1 = ParserFactory.get_available_parsers(FormType.FK_1)
    assert isinstance(parsers_fk1, dict)
    assert "Раздел0" in parsers_fk1

    # Для 5ФК должен возвращаться словарь с FiveFKParser
    parsers_fk5 = ParserFactory.get_available_parsers(FormType.FK_5)
    assert "*" in parsers_fk5
    assert parsers_fk5["*"] == FiveFKParser

    # Для UNKNOWN должен возвращаться UniversalParser
    parsers_unknown = ParserFactory.get_available_parsers(FormType.UNKNOWN)
    assert "*" in parsers_unknown
    assert parsers_unknown["*"] == UniversalParser


if __name__ == "__main__":
    pytest.main([__file__, "-v"])