"""Сервис агрегата Log: запись логов по единой схеме LogEntry."""
from datetime import datetime
from typing import Optional

from bson import ObjectId

from app.domain.log.models import LogEntry
from app.domain.log.repository import LogRepository


class LogService:
    def __init__(self, repository: LogRepository):
        self._repo = repository

    async def save_log(
        self,
        message: str,
        level: str = "info",
        logger: Optional[str] = None,
        pathname: Optional[str] = None,
        lineno: Optional[int] = None,
    ) -> None:
        entry = LogEntry(
            timestamp=datetime.utcnow(),
            level=level,
            message=message,
            logger=logger,
            pathname=pathname,
            lineno=lineno,
        )
        log_doc = {"_id": str(ObjectId()), **entry.to_mongo_doc()}
        await self._repo.insert_one(log_doc)
