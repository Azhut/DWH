"""Модели агрегата Form: тип формы, информация о форме."""
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class FormType(str, Enum):
    FK_1 = "1ФК"
    FK_5 = "5ФК"
    UNKNOWN = "unknown"


class FormInfo(BaseModel):
    """Информация о форме с определением типа. Реквизиты (skip_sheets и др.) — часть сущности Form."""
    id: str
    name: str
    type: FormType = Field(default=FormType.UNKNOWN, description="Тип формы")
    requisites: dict = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @classmethod
    def from_mongo_doc(cls, doc: dict) -> "FormInfo":
        if doc is None:
            raise ValueError("Document cannot be None")
        form_type = detect_form_type(doc.get("name", ""))
        requisites = dict(doc.get("requisites", {}))
        if "skip_sheets" not in requisites and "skip_sheets" in doc:
            requisites["skip_sheets"] = doc["skip_sheets"]
        return cls(
            id=doc.get("id", ""),
            name=doc.get("name", ""),
            type=form_type,
            requisites=requisites,
            created_at=doc.get("created_at", datetime.utcnow()),
        )


def detect_form_type(form_name: str) -> FormType:
    if not form_name or not isinstance(form_name, str):
        return FormType.UNKNOWN
    form_name_lower = form_name.lower()
    if "5фк" in form_name_lower:
        return FormType.FK_5
    if "1фк" in form_name_lower:
        return FormType.FK_1
    if "фк" in form_name_lower:
        # «ФК» без 1/5: считаем 1ФК только если перед «фк» нет цифры 2,3,4,6,7,8,9
        idx = form_name_lower.index("фк")
        if idx > 0 and form_name_lower[idx - 1] in "2346789":
            return FormType.UNKNOWN
        return FormType.FK_1
    return FormType.UNKNOWN
