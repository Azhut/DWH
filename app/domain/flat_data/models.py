"""Модели агрегата FlatData: запись плоских данных, константы полей и маппинг фильтров."""
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field


# Поля документа в коллекции FlatData (порядок для проекции и таблицы).
TABLE_FIELDS: List[str] = ["year", "reporter", "section", "row", "column", "value"]

# Маппинг имён фильтров API (рус.) -> имена полей в БД.
FILTER_MAP: Dict[str, str] = {
    "год": "year",
    "субъект": "reporter",
    "раздел": "section",
    "строка": "row",
    "колонка": "column",
}


class FlatDataRecord(BaseModel):
    """Одна запись flat_data (документ в коллекции FlatData)."""

    year: Optional[int] = None
    reporter: Optional[str] = None
    section: Optional[str] = None
    row: Optional[str] = None
    column: Optional[str] = None
    value: Optional[Union[int, float, str]] = None
    file_id: Optional[str] = None
    form: Optional[str] = None

    model_config = ConfigDict(extra="forbid")

    def to_mongo_doc(self) -> Dict[str, Any]:
        """Словарь для записи в MongoDB (без None, если нужно — можно оставить)."""
        return self.model_dump(exclude_none=False)

    @classmethod
    def from_mongo_doc(cls, doc: Dict[str, Any]) -> "FlatDataRecord":
        """Создать запись из документа MongoDB (лишние поля игнорируются)."""
        year = doc.get("year")
        if year is not None and isinstance(year, float):
            year = int(year)
        return cls(
            year=year,
            reporter=doc.get("reporter"),
            section=doc.get("section"),
            row=doc.get("row"),
            column=doc.get("column"),
            value=doc.get("value"),
            file_id=doc.get("file_id"),
            form=doc.get("form"),
        )


class FilterSpec(BaseModel):
    """Один применённый фильтр (filter-name + values)."""

    filter_name: str = Field(..., alias="filter-name")
    values: List[Union[str, int, float]] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    def to_query_field_and_values(self) -> tuple[str, List[Union[str, int, float]]]:
        """Возвращает (поле в БД, список значений) для построения запроса."""
        key = self.filter_name.lower()
        if key not in FILTER_MAP:
            raise ValueError(f"Неизвестный фильтр: {self.filter_name}")
        return FILTER_MAP[key], list(self.values)
