"""
Модели для работы с типами форм
"""
from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class FormType(str, Enum):
    """Типы статистических форм"""
    FK_1 = "1ФК"
    FK_5 = "5ФК"
    UNKNOWN = "unknown"


class FormInfo(BaseModel):
    """Информация о форме с определением типа"""
    id: str
    name: str
    type: FormType = Field(default=FormType.UNKNOWN, description="Тип формы")
    requisites: dict = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @classmethod
    def from_mongo_doc(cls, doc: dict) -> "FormInfo":
        """Создает FormInfo из документа MongoDB с определением типа"""
        if doc is None:
            raise ValueError("Document cannot be None")

        # Определяем тип формы по названию
        form_type = detect_form_type(doc.get("name", ""))

        return cls(
            id=doc.get("id", ""),
            name=doc.get("name", ""),
            type=form_type,
            requisites=doc.get("requisites", {}),
            created_at=doc.get("created_at", datetime.utcnow())
        )


def detect_form_type(form_name: str) -> FormType:
    """
    Определяет тип формы по ее названию.

    Правила:
    - Содержит "5ФК" -> FormType.FK_5
    - Содержит "1ФК" или "ФК" -> FormType.FK_1 (по умолчанию для совместимости)
    - Иначе -> FormType.UNKNOWN
    """
    if not form_name or not isinstance(form_name, str):
        return FormType.UNKNOWN

    form_name_lower = form_name.lower()

    # Порядок важен: сначала ищем 5ФК, потом 1ФК
    if "5фк" in form_name_lower:
        return FormType.FK_5
    elif "1фк" in form_name_lower or "фк" in form_name_lower:
        return FormType.FK_1

    return FormType.UNKNOWN