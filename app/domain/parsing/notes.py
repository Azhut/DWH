"""
Обработка примечаний (Справочно) в листах. Применяется только к форме 1ФК.
"""
import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def process_notes_1fk(sheet: pd.DataFrame, header_rows_count: int) -> pd.DataFrame:
    """
    Обрабатывает примечания в листе (блок «Справочно»). Только для 1ФК.

    Args:
        sheet: DataFrame листа
        header_rows_count: Количество строк заголовков (обычно 7 для 1ФК)

    Returns:
        DataFrame с развёрнутыми примечаниями в виде строк или исходный sheet при ошибке
    """
    try:
        from app.parsers.notes_processor import NotesProcessor

        return NotesProcessor.process_notes(sheet, raw_quantity=header_rows_count)
    except Exception as e:
        logger.warning("Не удалось обработать примечания (1ФК): %s", e)
        return sheet
