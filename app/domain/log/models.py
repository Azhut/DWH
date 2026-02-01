"""Модели агрегата Log: запись лога (stdout и MongoDB)."""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class LogEntry(BaseModel):
    """Одна запись лога (единая схема для приложения и Python logging → Mongo)."""

    timestamp: datetime = Field(default_factory=datetime.utcnow)
    level: str = "info"
    message: str = ""
    logger: Optional[str] = None
    pathname: Optional[str] = None
    lineno: Optional[int] = None

    def to_mongo_doc(self) -> dict:
        """Словарь для записи в MongoDB (без _id; _id добавляется при вставке)."""
        return self.model_dump(exclude_none=True)
