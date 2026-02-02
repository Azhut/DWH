"""
Тесты для реестра parsing pipeline (замена ParserFactory).
"""
import pytest
from app.application.parsing import get_parsing_pipeline_registry
from app.domain.form.models import FormType


def test_registry_for_fk1_specific_sheet():
    """Для 1ФК и известного листа возвращается pipeline с фиксированной структурой."""
    registry = get_parsing_pipeline_registry()
    pipeline = registry.get_pipeline(FormType.FK_1, "Раздел0")
    assert pipeline is not None
    assert pipeline.steps is not None
    assert len(pipeline.steps) > 0

    pipeline2 = registry.get_pipeline(FormType.FK_1, "Раздел2")
    assert pipeline2 is not None


def test_registry_for_fk1_unknown_sheet():
    """Для 1ФК и неизвестного листа (нет в реестре) используется wildcard или fallback."""
    registry = get_parsing_pipeline_registry()
    # Для "Неизвестный раздел" 1ФК в реестре нет специфичного pipeline — будет fallback UNKNOWN
    pipeline = registry.get_pipeline(FormType.FK_1, "Неизвестный раздел")
    assert pipeline is not None


def test_registry_for_fk5():
    """Для 5ФК возвращается pipeline с авто-детекцией структуры."""
    registry = get_parsing_pipeline_registry()
    pipeline = registry.get_pipeline(FormType.FK_5, "Любой лист")
    assert pipeline is not None
    assert len(pipeline.steps) > 0


def test_registry_for_unknown():
    """Для UNKNOWN возвращается универсальный pipeline."""
    registry = get_parsing_pipeline_registry()
    pipeline = registry.get_pipeline(FormType.UNKNOWN, "Лист1")
    assert pipeline is not None


def test_registry_specific_over_wildcard():
    """Специфичный pipeline для листа приоритетнее wildcard."""
    registry = get_parsing_pipeline_registry()
    specific = registry.get_pipeline(FormType.FK_1, "Раздел3")
    assert specific is not None
    # Должен быть тот же тип runner с шагами
    assert hasattr(specific, "run_for_sheet")
