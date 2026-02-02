"""
Универсальная сборка FlatDataRecord из извлечённых данных листа.
"""
import math
from typing import List, Optional

from app.domain.flat_data.models import FlatDataRecord
from app.domain.parsing.models import ExtractedSheetData, SERVICE_EMPTY

from app.parsers.notes_processor import _SERVICE_EMPTY as LEGACY_SERVICE_EMPTY  # совместимость


def build_flat_data_records(
    data: ExtractedSheetData,
    *,
    year: Optional[int] = None,
    city: Optional[str] = None,
    section: str = "",
    file_id: Optional[str] = None,
    form_id: Optional[str] = None,
    skip_empty: bool = True,
) -> List[FlatDataRecord]:
    """
    Строит список FlatDataRecord из извлечённых данных.

    Args:
        data: Извлечённые данные листа
        year, city, section, file_id, form_id: Метаданные для записей
        skip_empty: Пропускать ячейки с пустым/служебным значением
    """
    records: List[FlatDataRecord] = []
    city_upper = (city or "").upper()

    for col in data.columns:
        col_header = col.column_header
        for cell in col.values:
            value = cell.value
            if skip_empty:
                if value is None:
                    continue
                if value == SERVICE_EMPTY or value == LEGACY_SERVICE_EMPTY:
                    continue
                if isinstance(value, float) and math.isnan(value):
                    continue
                if isinstance(value, str) and value.strip() in ("", "nan", "none"):
                    continue
            if isinstance(value, float) and math.isnan(value):
                value = 0
            if isinstance(value, float) and value == int(value):
                value = int(value)
            records.append(
                FlatDataRecord(
                    year=year,
                    city=city_upper or None,
                    section=section,
                    row=cell.row_header,
                    column=col_header,
                    value=value,
                    file_id=file_id,
                    form=form_id,
                )
            )
    return records
