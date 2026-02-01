"""Агрегат FlatData: модели, репозиторий, сервис (сохранение, удаление, фильтрация)."""
from app.domain.flat_data.models import (
    FILTER_MAP,
    TABLE_FIELDS,
    FlatDataRecord,
    FilterSpec,
)
from app.domain.flat_data.repository import FlatDataRepository
from app.domain.flat_data.service import FlatDataService

__all__ = [
    "FILTER_MAP",
    "TABLE_FIELDS",
    "FlatDataRecord",
    "FilterSpec",
    "FlatDataRepository",
    "FlatDataService",
]
